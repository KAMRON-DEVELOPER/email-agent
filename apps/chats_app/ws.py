import asyncio
from datetime import UTC, datetime
from typing import Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, WebSocket
from redis.asyncio.client import PubSub

from apps.chats_app.app_tasks import create_chat_message_task
from settings.dependencies import websocketDependency
from settings.my_redis import chat_cache_manager, pubsub_manager
from settings.my_websocket import WebSocketContextManager, chat_ws_manager
from utils.enums import ChatEvent
from utils.logger import logger

chat_ws_router = APIRouter()


@chat_ws_router.websocket("/home")
async def enter_home(websocket_dependency: websocketDependency):
    user_id: str = websocket_dependency.user_id.hex
    websocket: WebSocket = websocket_dependency.websocket

    message_handlers = {
        ChatEvent.goes_online: handle_goes_online,
        ChatEvent.goes_offline: handle_goes_offline,
        ChatEvent.typing_start: handle_typing_start,
        ChatEvent.typing_stop: handle_typing_stop,
        ChatEvent.sent_message: handle_sent_message,
        ChatEvent.created_chat: handle_created_chat,
    }

    async with WebSocketContextManager(
        websocket=websocket,
        user_id=user_id,
        connect_handler=chat_connect,
        disconnect_handler=chat_disconnect,
        pubsub_generator=chat_pubsub_generator,
        message_handlers=message_handlers,
    ) as connection:
        await connection.wait_until_disconnected()


# Connection setup
async def chat_connect(user_id: str, websocket: WebSocket):
    await chat_ws_manager.connect(user_id=user_id, websocket=websocket)
    results: tuple[set[str], set[str]] = await chat_cache_manager.add_user_to_chats(user_id=user_id)
    if all(results):
        logger.debug("results has some data")
        tasks = [
            pubsub_manager.publish(topic=f"chats:home:{pid}", data={"id": chid, "type": ChatEvent.goes_online.value})
            for chid, pid in zip(results[0], results[1])
        ]
        await asyncio.gather(*tasks)


async def chat_disconnect(user_id: str, websocket: WebSocket):
    await chat_ws_manager.disconnect(user_id=user_id, websocket=websocket)
    results: tuple[set[str], set[str]] = await chat_cache_manager.remove_user_from_chats(user_id=user_id)
    if all(results):
        tasks = [
            pubsub_manager.publish(topic=f"chats:home:{pid}", data={"id": chid, "type": ChatEvent.goes_offline.value})
            for chid, pid in zip(results[0], results[1])
        ]
        await asyncio.gather(*tasks)


async def chat_pubsub_generator(user_id: str) -> PubSub:
    return await pubsub_manager.subscribe(topic=f"chats:home:{user_id}")


# Event handlers
async def handle_goes_online(user_id: str, data: dict[str, str]):
    await chat_ws_manager.send_personal_message(user_id=user_id, data=data)


async def handle_goes_offline(user_id: str, data: dict):
    await chat_ws_manager.send_personal_message(user_id=user_id, data=data)


async def handle_typing_start(user_id: str, data: dict):
    logger.debug(f"User started typing in {data.get('id')}")
    chat_id: Optional[str] = data.get("id")
    if not chat_id:
        await chat_ws_manager.send_personal_message(user_id=user_id, data={"detail": "You must provider chat id!"})

    online_participants: set[str] = await chat_cache_manager.get_chat_participants(chat_id=chat_id, user_id=user_id, online=True)
    if online_participants:
        tasks = [chat_ws_manager.send_personal_message(user_id=pid, data=data) for pid in online_participants]
        await asyncio.gather(*tasks)


async def handle_typing_stop(user_id: str, data: dict):
    logger.debug(f"User stopped typing in {data.get('id')}")
    chat_id: Optional[str] = data.get("id")
    if not chat_id:
        await chat_ws_manager.send_personal_message(user_id=user_id, data={"detail": "You must provider chat id!"})

    online_participants: set[str] = await chat_cache_manager.get_chat_participants(chat_id=chat_id, user_id=user_id, online=True)
    if online_participants:
        tasks = [chat_ws_manager.send_personal_message(user_id=pid, data=data) for pid in online_participants]
        await asyncio.gather(*tasks)


async def handle_sent_message(user_id: str, data: dict):
    logger.warning(f"handle_sent_message data: {data}")

    participant_id: Optional[str] = data.get("participant", {}).get("id", None)
    if not participant_id:
        logger.error("Missing participant id in sent_message event.")
        return

    chat_id: str = data.get("id", "")
    message = data.get("last_message", {}).get("message", "")

    message_id: UUID = uuid4()
    now = datetime.now(UTC)
    now_timestamp = int(now.timestamp())

    await create_chat_message_task.kiq(message_id=message_id, user_id=UUID(hex=user_id), chat_id=UUID(hex=chat_id), message=message)

    mapping = {
        "id": chat_id,
        "last_activity_at": data.get("last_activity_at", now_timestamp),
        "last_message": {
            "id": message_id.hex,
            "chat_id": chat_id,
            "sender_id": data.get("last_message", {}).get("sender_id"),
            "message": message,
            "created_at": data.get("last_message", {}).get("created_at", now_timestamp),
        },
    }

    await chat_cache_manager.create_chat(user_id=user_id, participant_id=participant_id, chat_id=chat_id, mapping=mapping)

    logger.debug(f"data type: {type(data)}")

    await chat_ws_manager.send_personal_message(user_id=user_id, data=data)
    await chat_ws_manager.send_personal_message(user_id=participant_id, data=data)


async def handle_created_chat(user_id: str, data: dict):
    participant_id = data.get("participant", {}).get("id")
    chat_id = data.get("id")
    logger.debug(f"User {participant_id} created a chat room (ID: {chat_id}) with you ({user_id})")
    await chat_ws_manager.send_personal_message(user_id=user_id, data=data)
