import asyncio
import base64
import json
from base64 import urlsafe_b64decode
from email.mime.text import MIMEText
from typing import AsyncIterable, Optional
from uuid import UUID

from bs4 import BeautifulSoup
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message
from google.cloud.pubsub_v1.types import FlowControl
from google.oauth2.credentials import Credentials
from google.pubsub_v1 import StreamingPullResponse, SubscriberAsyncClient
from google.pubsub_v1.types import StreamingPullRequest
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from sqlalchemy import select

from services.schemas import History, ReceivedMessageData, MessageResponse
from settings.classifier import EmailClassifier
from settings.config import get_settings
from settings.database import get_db_session
from settings.models import AccountModel, EmailCategory, EmailModel, ImportanceLevel, RefundRequestModel, UnhandledEmailModel
from utils.logger import logger

settings = get_settings()
classifier = EmailClassifier(model_path=None)


def get_google_auth_flow():
    """Initializes and returns the Google OAuth Flow."""
    flow = Flow.from_client_secrets_file(
        client_secrets_file=settings.CLIENT_SECRETS_FILE,
        scopes=settings.SCOPES,
        redirect_uri=settings.REDIRECT_URI,
    )
    return flow


def get_user_info(credentials: Credentials):
    """Fetches user info using their credentials."""
    service = build("oauth2", "v2", credentials=credentials)
    user_info = service.userinfo().get().execute()
    return user_info


def start_watching_gmail(credentials: Credentials):
    try:
        gmail_service = build("gmail", "v1", credentials=credentials)
        request = {"labelIds": ["INBOX"], "topicName": settings.TOPIC_NAME}
        response = gmail_service.users().watch(userId="me", body=request).execute()
        logger.debug("Watch response:", response)
        return response["historyId"]
    except Exception as e:
        logger.exception(f"Exception in start_watching_gmail, e: {e}")


def stop_watching_gmail(credentials: Credentials):
    gmail_service = build("gmail", "v1", credentials=credentials)
    gmail_service.users().stop(userId="me").execute()


def send_email(credentials, to_email, subject, body_text):
    service = build("gmail", "v1", credentials=credentials)

    message = MIMEText(body_text)
    message["to"] = to_email
    message["subject"] = subject

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    send_result = service.users().messages().send(userId="me", body={"raw": raw_message}).execute()

    return send_result


async def process_message(account_id: UUID, message: MessageResponse):
    payload = message.payload
    headers = {h.name: h.value for h in payload.headers}
    subject = headers.get("Subject", "")
    from_email = headers.get("From", "")
    to_email = headers.get("To", "")

    body = ""
    if payload.parts:
        for part in payload.parts:
            if part.mimeType == "text/plain" and part.body.data:
                body = urlsafe_b64decode(part.body.data).decode("utf-8")
            elif part.mimeType == "text/html" and part.body.data:
                html = urlsafe_b64decode(part.body.data).decode("utf-8")
                body = BeautifulSoup(html, "html.parser").get_text()

    # Classify email
    category, confidence = classifier.categorize_email(subject=subject, body=body)
    logger.debug(f"subject: {subject}, body: {body}")
    logger.debug(f"category: {category}, confidence: {confidence}")

    # Save in DB
    async with get_db_session() as session:
        email_model = EmailModel(
            message_id=message.id,
            thread_id=message.threadId,
            subject=subject,
            body=body,
            from_email=from_email,
            to_email=to_email,
            category=EmailCategory[category],
            account_id=account_id,
        )

        session.add(email_model)
        await session.commit()
        await session.refresh(email_model)

        if category == "refund":
            refund_request = RefundRequestModel(email=email_model)
            session.add(refund_request)
        elif category == "question":
            # TODO: handle questions (RAG, etc.)
            pass
        elif category == "other":
            unhandled = UnhandledEmailModel(email=email_model, reason="Unknown", importance=ImportanceLevel.low)
            session.add(unhandled)

        await session.commit()


async def get_new_messages(account_id: UUID, start_history_id, credentials: Credentials):
    service = build("gmail", "v1", credentials=credentials)
    history_response: dict = service.users().history().list(userId="me", startHistoryId=start_history_id).execute()
    history = History(**history_response)
    logger.debug(f"history_response: {history_response}, type: {type(history_response)}")

    for record in history.history:
        if record.messages:
            for message in record.messages:
                message_response: dict = service.users().messages().get(userId="me", id=message.id, format="full").execute()
                logger.debug(f"message_response: {message_response}, type: {type(message_response)}")
                message = MessageResponse(**message_response)
                await process_message(message=message, account_id=account_id)

        if record.labelsRemoved:
            logger.debug(f"Labels removed: {record.labelsRemoved}")


async def fetch_new_messages(received_message_data: ReceivedMessageData):
    email_address = received_message_data.emailAddress
    history_id = received_message_data.historyId
    if not email_address or not history_id:
        logger.warning("Invalid Pub/Sub payload")
        return

    async with get_db_session() as session:
        stmt = select(AccountModel).where(AccountModel.gmail_address == email_address)
        result = await session.execute(stmt)
        account: Optional[AccountModel] = result.scalar_one_or_none()

        if not account:
            logger.warning(f"No account found for {email_address}")
            return

        credentials = Credentials.from_authorized_user_info(info=json.loads(account.credentials))
        await get_new_messages(account_id=account.id, start_history_id=history_id, credentials=credentials)


async def handle_received_message(data: str) -> None:
    """Async processing of a message"""
    try:
        payload: dict = json.loads(data)
        logger.info(f"✅ Parsed Pub/Sub payload: {payload}")
        received_message_data = ReceivedMessageData(**payload)
        await fetch_new_messages(received_message_data=received_message_data)
    except Exception as e:
        logger.error(f"Error handling Pub/Sub message: {e}")


# ------------------------------------- PubSubListenerAsync -------------------------------------
class PubSubListenerAsync:
    def __init__(self, project_id: str, subscription_id: str, service_account_file: str):
        self.subscriber = SubscriberAsyncClient.from_service_account_file(filename=service_account_file)
        self.subscription_path = self.subscriber.subscription_path(project=project_id, subscription=subscription_id)
        self._stream: Optional[AsyncIterable[StreamingPullResponse]] = None
        self._is_shutdown = False

    async def listen(self) -> None:
        """Start listening using StreamingPull with an automatic retry loop."""
        while not self._is_shutdown:
            try:

                async def request_generator():
                    yield StreamingPullRequest(
                        subscription=self.subscription_path, max_outstanding_messages=100, max_outstanding_bytes=10_000_000, stream_ack_deadline_seconds=60
                    )

                    while not self._is_shutdown:
                        await asyncio.sleep(30)
                        yield StreamingPullRequest()

                self._stream = await self.subscriber.streaming_pull(requests=request_generator())
                logger.info(f" 🎯 Listening for messages on {self.subscription_path}...")

                async for response in self._stream:
                    for received_message in response.received_messages:
                        try:
                            data = received_message.message.data.decode("utf-8")
                            logger.info(f" 📩 New Pub/Sub message.attributes(dict): {received_message.message.attributes}")
                            logger.info(f" 📩 New Pub/Sub message.message_id: {received_message.message.message_id}")
                            logger.info(f" 📩 New Pub/Sub message.ordering_key: {received_message.message.ordering_key}")
                            logger.info(f" 📩 New Pub/Sub message.publish_time: {received_message.message.publish_time}")
                            logger.info(f" 📩 type of message.data: {type(received_message.message.data)}")
                            logger.info(f" 📩 New Pub/Sub message.data: {data}, type: {type(data)}")
                            await handle_received_message(data)
                            await self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[received_message.ack_id])
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            # Nack, redelivery
                            await self.subscriber.modify_ack_deadline(
                                subscription=self.subscription_path, ack_ids=[received_message.ack_id], ack_deadline_seconds=0
                            )

            except asyncio.CancelledError:
                logger.info("Listener task was cancelled.")
                self._is_shutdown = True
                break
            except GoogleAPICallError as e:
                logger.error(f"Google API streaming error: {e}. Retrying in 30 seconds...")
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"An unexpected error occurred in the listener: {e}. Retrying in 30 seconds...")
                await asyncio.sleep(30)

    async def shutdown(self):
        """Gracefully shutdown the subscriber"""
        logger.info("Shutting down Pub/Sub async subscriber...")
        self._is_shutdown = True
        if self._stream:
            self._stream = None
        logger.info("Async subscriber fully cleaned up")


# ------------------------------------- PubSubListener -------------------------------------
def callback(message: Message) -> None:
    """Sync callback for Pub/Sub messages"""
    try:
        logger.info(f"📩 Raw Pub/Sub message: {message.data}")
        payload = json.loads(message.data.decode("utf-8"))
        logger.info(f"✅ Parsed Pub/Sub payload: {payload}")
        # Run your processing logic (can be sync or thread-safe async scheduling)
        fetch_new_messages(payload)
        message.ack()
    except Exception as e:
        logger.error(f"❌ Error handling Pub/Sub message: {e}")
        message.nack()


class PubSubListener:
    def __init__(self, project_id: str, subscription_id: str, service_account_file: str):
        self.subscriber = SubscriberClient.from_service_account_file(filename=service_account_file)
        self.subscription_path = self.subscriber.subscription_path(project=project_id, subscription=subscription_id)
        self._future: Optional[StreamingPullFuture] = None

    def listen(self, timeout: float | None = None) -> None:
        """Start listening with a sync subscriber"""
        flow_control = FlowControl(max_messages=100)
        self._future = self.subscriber.subscribe(self.subscription_path, callback=callback, flow_control=flow_control)
        logger.info(f" 🎯 Listening for messages on {self.subscription_path}...")

        with self.subscriber:
            try:
                self._future.result(timeout=timeout)
            except TimeoutError:
                self._future.cancel()
                logger.info("Listener stopped due to timeout")
            except Exception as e:
                logger.error(f"Subscription error: {e}")
                self._future.cancel()
