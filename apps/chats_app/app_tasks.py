from typing import Annotated
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from taskiq import TaskiqDepends

# from apps.chats_app.models import ChatMessageModel
from settings.database import get_session
from settings.my_taskiq import broker
from utils.logger import logger


@broker.task(task_name="create_chat_message_task")
async def create_chat_message_task(message_id: UUID, user_id: UUID, chat_id: UUID, message: str, session: Annotated[AsyncSession, TaskiqDepends(get_session)]):
    """(message_id: str, user_id: str, chat_id: str, message: str) fields required"""
    try:
        pass
        # message = ChatMessageModel(id=message_id, chat_id=chat_id, sender_id=user_id, message=message)
        # session.add(instance=message)
        # await session.commit()
    except Exception as e:
        logger.exception(f"Exception in create_chat_message_task, e: {e}")
