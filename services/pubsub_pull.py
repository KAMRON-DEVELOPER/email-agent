import asyncio
import json
from typing import AsyncIterable, Optional

from google.api_core.exceptions import GoogleAPICallError
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message
from google.cloud.pubsub_v1.types import FlowControl
from google.pubsub_v1 import StreamingPullResponse, SubscriberAsyncClient
from google.pubsub_v1.types import StreamingPullRequest

from services.gmail_fetch import fetch_new_messages
from utils.logger import logger


async def handle_message(data: str) -> None:
    """Async processing of a message"""
    try:
        payload = json.loads(data)
        logger.info(f"✅ Parsed Pub/Sub payload: {payload}")
        await fetch_new_messages(payload)
    except Exception as e:
        logger.error(f"❌ Error handling Pub/Sub message: {e}")


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
                            logger.info(f" 📩 New Pub/Sub message: {data}")
                            await handle_message(data)
                            await self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[received_message.ack_id])
                        except Exception as e:
                            logger.error(f"❌ Error processing message: {e}")
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
                logger.info("⏹️ Listener stopped due to timeout")
            except Exception as e:
                logger.error(f"❌ Subscription error: {e}")
                self._future.cancel()
