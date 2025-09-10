import asyncio
import json
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from google.oauth2.credentials import Credentials
from sqlalchemy import select
from starlette.middleware.sessions import SessionMiddleware

from services.google_apis import PubSubListener, PubSubListenerAsync, get_google_auth_flow, get_user_info, start_watching_gmail, stop_watching_gmail
from settings.config import get_settings
from settings.database import DBSession, initialize_db
from settings.models import AccountModel
from utils.logger import logger

settings = get_settings()

listener = PubSubListenerAsync(
    project_id=settings.PROJECT_ID, subscription_id=settings.SUBSCRIPTION_ID, service_account_file=str(settings.SERVICE_ACCOUNT_FILE_PATH)
)

sync_listener = PubSubListener(
    project_id=settings.PROJECT_ID, subscription_id=settings.SUBSCRIPTION_ID, service_account_file=str(settings.SERVICE_ACCOUNT_FILE_PATH)
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.warning("🚀 Starting app_lifespan...")
    try:
        await initialize_db()
    except Exception as e:
        logger.exception(f"DB initialization exception, e: {e}")

    task = asyncio.create_task(listener.listen())

    # thread = Thread(target=sync_listener.listen, daemon=True)
    # thread.start()

    yield

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("🛑 Listener task cancelled during shutdown")
    await listener.shutdown()


app = FastAPI(lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=settings.SECRET_KEY)
templates = Jinja2Templates(directory="templates")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("static/favicon.ico")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request, session: DBSession):
    session_id = request.session.get("session_id")
    if not session_id:
        session_id = str(uuid4())
        request.session["session_id"] = session_id

    stmt = select(AccountModel).where(AccountModel.session_id == session_id)
    result = await session.execute(stmt)
    accounts = result.scalars().all()
    return templates.TemplateResponse("index.html", {"request": request, "accounts": accounts})


@app.get("/auth/google")
async def google_auth_redirect(request: Request):
    flow = get_google_auth_flow()
    authorization_url, state = flow.authorization_url(access_type="offline", prompt="consent")
    logger.debug(f"authorization_url: {authorization_url}, state: {state}")
    request.session["state"] = state
    return RedirectResponse(url=authorization_url)


@app.get("/auth/callback")
async def google_auth_callback(request: Request, code: str, state: str, session: DBSession):
    logger.debug(f"request.session: {request.session}")
    logger.debug(f"code: {code}")
    logger.debug(f"state: {state}")
    logger.debug(f"request.session.get('state'): {request.session.get('state')}")

    session_state = request.session.get("state")
    if not session_state or state != session_state:
        raise HTTPException(status_code=400, detail="Invalid state parameter.")

    session_id = request.session.get("session_id")
    if not session_id:
        raise HTTPException(status_code=403, detail="No active session found.")

    try:
        flow = get_google_auth_flow()
        flow.fetch_token(code=code)
        credentials = flow.credentials
        logger.debug(f"credentials: {credentials.to_json()}")

        user_info = get_user_info(credentials)
        user_email = user_info.get("email")

        logger.debug(f"user_info: {user_info}, user_email: {user_email}")

        if not user_email:
            raise HTTPException(status_code=400, detail="Could not retrieve email.")

        stmt = select(AccountModel).where(AccountModel.session_id == session_id, AccountModel.gmail_address == user_email)
        result = await session.execute(stmt)
        existing_account = result.scalars().first()

        if existing_account:
            existing_account.credentials = credentials.to_json()
            logger.info(f"Updated credentials for {user_email}")
        else:
            new_account = AccountModel(session_id=session_id, gmail_address=user_email, credentials=credentials.to_json())
            session.add(new_account)
            logger.info(f"Added new account {user_email}")

        await session.commit()

        history_id = start_watching_gmail(credentials=credentials)
        logger.info(f"Started watching Gmail for {user_email}, history_id={history_id}")
    except Exception as e:
        logger.error(f"Error during Google auth callback: {e}")
        raise HTTPException(status_code=500, detail="Authentication failed.")

    return RedirectResponse(url="/", status_code=303)


@app.post("/disconnect/{account_id}")
async def disconnect_account(request: Request, account_id: UUID, session: DBSession):
    session_id = request.session.get("session_id")
    if not session_id:
        raise HTTPException(status_code=403, detail="No active session.")

    stmt = select(AccountModel).where(AccountModel.id == account_id, AccountModel.session_id == session_id)
    result = await session.execute(stmt)
    account = result.scalars().first()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found.")

    credentials = Credentials.from_authorized_user_info(info=json.loads(account.credentials))
    stop_watching_gmail(credentials=credentials)

    await session.delete(account)
    await session.commit()

    logger.info(f"Stopped Gmail watch and disconnected {account.gmail_address}")
    return RedirectResponse(url="/", status_code=303)
