import json
import time
from datetime import UTC, datetime
from typing import Optional
from uuid import UUID

from coredis import PureToken
from coredis import Redis as SearchRedis
from coredis.exceptions import ResponseError
from coredis.modules.search import Field
from redis.asyncio import Redis as CacheRedis
from redis.asyncio.client import PubSub

from apps.chats_app.schemas import ChatMessageSchema, ChatResponseSchema, ChatSchema, ParticipantSchema
from settings.config import get_settings
from utils.enums import EngagementType
from utils.logger import logger

settings = get_settings()

my_cache_redis: CacheRedis = CacheRedis(
    host=settings.REDIS_HOST,
    db=0,
    decode_responses=True,
    auto_close_connection_pool=True,
    ssl=True,
    ssl_ca_certs=str(settings.CA_PATH),
    ssl_certfile=str(settings.CLIENT_CERT_PATH),
    ssl_keyfile=str(settings.CLIENT_KEY_PATH),
    ssl_cert_reqs="required",
    ssl_check_hostname=True,
)
my_search_redis: SearchRedis = SearchRedis(
    host=settings.REDIS_HOST,
    db=0,
    decode_responses=True,
    ssl=True,
    ssl_ca_certs=str(settings.CA_PATH),
    ssl_certfile=str(settings.CLIENT_CERT_PATH),
    ssl_keyfile=str(settings.CLIENT_KEY_PATH),
    ssl_cert_reqs="required",
    ssl_check_hostname=True,
)

USER_INDEX_NAME = "idx:users"
feed_INDEX_NAME = "idx:feeds"


async def redis_ready() -> bool:
    try:
        my_cache_redis_ping_result = await my_cache_redis.ping()
        my_search_redis_ping_result = await my_search_redis.ping()
        logger.debug(f"my_cache_redis_ping_result: {my_cache_redis_ping_result}")
        logger.debug(f"my_search_redis_ping_result: {my_search_redis_ping_result}")
        return True
    except Exception as e:
        logger.exception(f"redis_ready: {e}")
        return False


async def initialize_redis_indexes() -> None:
    try:
        logger.debug(f"creating index...")
        await my_search_redis.search.create(
            index=USER_INDEX_NAME, on=PureToken.HASH, schema=[Field("email", PureToken.TEXT), Field("username", PureToken.TEXT)], prefixes=["users:"]
        )
        logger.info("User index created/updated")
    except ResponseError as e:
        if "Index already exists" not in str(e):
            raise

    try:
        await my_search_redis.search.create(index=feed_INDEX_NAME, on=PureToken.HASH, schema=[Field("body", PureToken.TEXT)], prefixes=["feeds:"])
        logger.info("Feed index created/updated")
    except ResponseError as e:
        if "Index already exists" not in str(e):
            raise


class RedisPubSubManager:
    def __init__(self, cache_redis: CacheRedis):
        self.cache_redis = cache_redis
        self.active_subscriptions: dict[str, PubSub] = {}

    async def publish(self, topic: str, data: dict):
        await self.cache_redis.publish(channel=topic, message=json.dumps(data))

    async def subscribe(self, topic: str) -> PubSub:
        pubsub = self.cache_redis.pubsub()
        await pubsub.subscribe(topic)
        self.active_subscriptions[topic] = pubsub
        return pubsub

    async def unsubscribe(self, topic: str):
        if pubsub := self.active_subscriptions.get(topic):
            try:
                await pubsub.unsubscribe(topic)
                await pubsub.close()
            finally:
                self.active_subscriptions.pop(topic, None)


class ChatCacheManager:
    def __init__(self, cache_redis: CacheRedis, search_redis: SearchRedis):
        self.cache_redis = cache_redis
        self.search_redis = search_redis

    async def create_chat(self, user_id: str, participant_id: str, chat_id: str, mapping: dict):
        last_message: dict = mapping.pop("last_message")
        now_timestamp = datetime.now(UTC).timestamp()
        async with self.cache_redis.pipeline() as pipe:
            pipe.zadd(name=f"users:{user_id}:chats", mapping={chat_id: now_timestamp})
            pipe.zadd(name=f"users:{participant_id}:chats", mapping={chat_id: now_timestamp})
            pipe.hset(name=f"chats:{chat_id}:meta", mapping=mapping)
            pipe.hset(name=f"chats:{chat_id}:last_message", mapping=last_message)
            pipe.sadd(f"chats:{chat_id}:participants", user_id, participant_id)
            await pipe.execute()

    async def delete_chat(self, participants: list[str], chat_id: str):
        async with self.cache_redis.pipeline() as pipe:
            for pid in participants:
                pipe.zrem(f"users:{pid}:chats", chat_id)
                pipe.delete(f"chats:{chat_id}:meta")
                pipe.delete(f"chats:{chat_id}:last_message")
            pipe.srem(f"chats:{chat_id}:participants", *participants)
            await pipe.execute()

    async def get_chats(self, user_id: str, start: int = 0, end: int = 20) -> ChatResponseSchema:
        chat_ids: list[str] = await self.cache_redis.zrevrange(name=f"users:{user_id}:chats", start=start, end=end)
        if not chat_ids:
            return ChatResponseSchema(chats=[], end=0)

        async with self.cache_redis.pipeline() as pipe:
            for chat_id in chat_ids:
                pipe.hgetall(f"chats:{chat_id}:meta")  # index 0, 3, 6...
                pipe.hgetall(f"chats:{chat_id}:last_message")  # index 1, 4, 7...
                pipe.smembers(f"chats:{chat_id}:participants")  # index 2, 5, 8...
            results = await pipe.execute()

        chats: list[dict] = results[::3]  # Every 3rd element starting at 0
        last_messages: list[dict] = results[1::3]  # Every 3rd element starting at 1
        participant_sets: list[set[str]] = results[2::3]  # Every 3rd element starting at 2

        participant_ids: list[str] = []
        for participant_set in participant_sets:
            participant_set.discard(user_id)
            pid: Optional[str] = next(iter(participant_set), None)
            if not pid:
                continue
            participant_ids.append(pid)

        async with self.cache_redis.pipeline() as pipe:
            for pid in participant_ids:
                pipe.hgetall(f"users:{pid}:profile")
            for pid in participant_ids:
                pipe.sismember("chats:online", pid)
            piped_results = await pipe.execute()

        profiles: list[dict] = piped_results[: len(participant_ids)]
        statuses: list[bool] = piped_results[len(participant_ids) :]

        chat_list = []
        for chat_meta, last_msg, pid, profile, is_online in zip(chats, last_messages, participant_ids, profiles, statuses):
            if not pid or not profile:
                continue

            chat = ChatSchema(
                id=chat_meta.get("id"),
                participant=ParticipantSchema(
                    id=UUID(hex=pid),
                    name=profile.get("name"),
                    username=profile.get("username"),
                    avatar_url=profile.get("avatar_url"),
                    last_seen_at=datetime.fromtimestamp(int(profile.get("last_seen_at"))) if "last_seen_at" in profile else None,
                    is_online=is_online,
                ),
                last_activity_at=datetime.fromtimestamp(float(chat_meta.get("last_activity_at", time.time()))),
                last_message=ChatMessageSchema(
                    id=UUID(hex=last_msg.get("id", "")),
                    sender_id=UUID(hex=last_msg.get("sender_id", "")),
                    chat_id=UUID(hex=last_msg.get("chat_id", "")),
                    message=last_msg.get("message", ""),
                    created_at=datetime.fromtimestamp(float(last_msg.get("created_at", time.time()))),
                ),
            )
            chat_list.append(chat)

        return ChatResponseSchema(chats=chat_list, end=len(chat_ids) - 1)

    async def is_user_chat_owner(self, user_id: str, chat_id: str) -> bool:
        score: Optional[float] = await self.cache_redis.zscore(name=f"users:{user_id}:chats", value=chat_id)
        logger.warning(f"user_id: {user_id}")
        logger.warning(f"chat_id: {chat_id}")
        logger.warning(f"score: {score}")
        return False if score is None else True

    async def is_online(self, participant_id: str) -> bool:
        return bool(await self.cache_redis.sismember(name="chats:online", value=participant_id))

    """ ****************************************** EVENTS ****************************************** """

    async def add_user_to_chats(self, user_id: str) -> tuple[set[str], set[str]]:
        chat_ids: list[str] = await self.cache_redis.zrevrange(name=f"users:{user_id}:chats", start=0, end=-1)

        async with self.cache_redis.pipeline() as pipe:
            pipe.sadd("chats:online", user_id)
            for chat_id in chat_ids:
                pipe.sinter(f"chats:{chat_id}:participants", "chats:online")
            results = await pipe.execute()

        online_participants: set[str] = set()
        chat_ids_with_online: set[str] = set()
        online_users_per_chat_results: list[set[str]] = results[1:]

        for chat_id, online_in_chat in zip(chat_ids, online_users_per_chat_results):
            other_online_users = {pid for pid in online_in_chat if pid != user_id}
            if other_online_users:
                chat_ids_with_online.add(chat_id)
                online_participants.update(other_online_users)

        return chat_ids_with_online, online_participants

    async def remove_user_from_chats(self, user_id: str) -> tuple[set[str], set[str]]:
        chat_ids: list[str] = await self.cache_redis.zrevrange(name=f"users:{user_id}:chats", start=0, end=-1)

        async with self.cache_redis.pipeline() as pipe:
            pipe.srem(f"chats:online", user_id)
            pipe.hset(f"users:{user_id}:profile", key="last_seen_at", value=int(datetime.now(UTC).timestamp()))
            for chat_id in chat_ids:
                pipe.sinter(f"chats:{chat_id}:participants", "chats:online")
            results = await pipe.execute()

        online_participants: set[str] = set()
        chat_ids_with_online: set[str] = set()
        online_users_per_chat_results: list[set[str]] = results[2:]

        for chat_id, online_in_chat in zip(chat_ids, online_users_per_chat_results):
            other_online_users = {pid for pid in online_in_chat if pid != user_id}
            if other_online_users:
                chat_ids_with_online.add(chat_id)
                online_participants.update(other_online_users)

        return chat_ids_with_online, online_participants

    async def get_chat_participants(self, chat_id: str, user_id: str | None = None, online: bool = False) -> set[str]:
        if online:
            participants = await self.cache_redis.sinter(f"chats:{chat_id}:participants", "chats:online")
            if user_id:
                participants.discard(user_id)
            return participants
        else:
            return await self.cache_redis.smembers(f"chats:{chat_id}:participants")


class CacheManager:
    def __init__(self, cache_redis: CacheRedis, search_redis: SearchRedis):
        self.cache_redis = cache_redis
        self.search_redis = search_redis

    USER_TIMELINE_KEY = "user:{user_id}:user_timeline"

    """ ****************************************** TIMELINE ****************************************** """

    async def get_discover_timeline(self, user_id: Optional[str] = None, start: int = 0, end: int = 10) -> dict[str, list[dict] | int]:
        total_count: int = await self.cache_redis.zcard(name="global_timeline")
        if total_count == 0:
            return {"feeds": [], "end": 0}

        feed_ids: list[str] = await self.cache_redis.zrevrange(name="global_timeline", start=start, end=end)
        feeds = await self._get_feeds(user_id=user_id, feed_ids=feed_ids)
        return {"feeds": feeds, "end": total_count}

    async def get_following_timeline(self, user_id: str, start: int = 0, end: int = 10) -> dict[str, list[dict] | int]:
        total_count: int = await self.cache_redis.zcard(name=f"users:{user_id}:following_timeline")
        if total_count == 0:
            return {"feeds": [], "end": 0}

        feed_ids: list[str] = list(await self.cache_redis.zrevrange(name=f"users:{user_id}:following_timeline", start=start, end=end))
        feeds: list[dict] = await self._get_feeds(user_id=user_id, feed_ids=feed_ids)
        return {"feeds": feeds, "end": total_count}

    async def get_user_timeline(self, user_id: str, engagement_type: EngagementType, start: int = 0, end: int = 10) -> dict[str, list[dict] | int]:
        prefix: str = "user_timeline" if engagement_type == EngagementType.feeds else engagement_type.value

        if engagement_type == EngagementType.feeds:
            total_count: int = await self.cache_redis.zcard(name=f"users:{user_id}:{prefix}")
        else:
            total_count: int = await self.cache_redis.scard(name=f"users:{user_id}:{prefix}")

        if total_count == 0:
            return {"feeds": [], "end": 0}

        if engagement_type == EngagementType.feeds:
            feed_ids = await self.cache_redis.zrevrange(name=f"users:{user_id}:{prefix}", start=start, end=end)
        else:
            all_feed_ids: set[str] = await self.cache_redis.smembers(name=f"users:{user_id}:{prefix}")
            feed_ids = list(all_feed_ids)[start : end + 1]
            # feed_ids = await self.cache_redis.lrange(name=f"users:{user_id}:{prefix}", start=start, end=end)

        feeds: list[dict] = await self._get_feeds(user_id=user_id, feed_ids=feed_ids)
        return {"feeds": feeds, "end": total_count}

    async def _get_feeds(self, feed_ids: list[str], user_id: Optional[str] = None) -> list[dict]:

        # Fetch feed metadata
        async with self.cache_redis.pipeline() as pipe:
            for feed_id in feed_ids:
                pipe.hgetall(f"feeds:{feed_id}:meta")
            feed_metas: list[dict] = await pipe.execute()

        # Process feed metadata and apply visibility filtering
        valid_feeds = []
        follower_check_authors = set()
        author_feed_map = {}

        # Process feed metadata
        for feed_meta in feed_metas:
            if not feed_meta:
                continue

            visibility = feed_meta.get("feed_visibility")
            author_id = feed_meta.get("author_id")

            # Public feeds always visible
            if visibility == "public":
                valid_feeds.append(feed_meta)

            # Private feeds only visible to owner
            elif visibility == "private":
                if user_id and user_id == author_id:
                    valid_feeds.append(feed_meta)

            # Followers-only feeds
            elif visibility == "followers":
                if not user_id:
                    continue  # Anonymous users can't see followers-only
                if user_id == author_id:
                    valid_feeds.append(feed_meta)  # Always visible to author
                else:
                    # Defer follower check
                    follower_check_authors.add(author_id)
                    author_feed_map.setdefault(author_id, []).append(feed_meta)

        # Batch process follower checks
        if follower_check_authors and user_id:
            follow_status = await self._check_followers_batch(user_id, list(follower_check_authors))
            for author_id, is_follower in follow_status.items():
                if is_follower:
                    valid_feeds.extend(author_feed_map[author_id])

        feeds = valid_feeds
        if not feeds:
            return []

        # Get all author IDs
        author_ids = {feed["author_id"] for feed in feeds}

        # 🛡️ Filter blocked authors
        if user_id:
            async with self.cache_redis.pipeline() as pipe:
                for author_id in author_ids:
                    pipe.sismember(f"users:{user_id}:blocked", author_id)
                    pipe.sismember(f"users:{author_id}:blocked", user_id)
                block_flags = await pipe.execute()

            # Build block map
            block_map = {}
            for i, author_id in enumerate(author_ids):
                blocked_by_user = bool(block_flags[i * 2])
                blocked_user = bool(block_flags[i * 2 + 1])
                block_map[author_id] = blocked_by_user or blocked_user

            feeds = [feed for feed in feeds if not block_map.get(feed["author_id"], False)]

            if not feeds:
                return []

        if not feeds:
            return []

        # Process engagement results
        engagement_keys = ["comments", "reposts", "quotes", "likes", "views", "bookmarks"]
        interaction_keys = ["reposted", "quoted", "liked", "viewed", "bookmarked"]

        async with self.cache_redis.pipeline() as pipe:
            for feed in feeds:
                feed_id = feed["id"]
                for key in engagement_keys:
                    pipe.scard(f"feeds:{feed_id}:{key}")
                if user_id:
                    for key in engagement_keys[1:]:
                        pipe.sismember(f"feeds:{feed_id}:{key}", user_id)
            results = await pipe.execute()

        for index, feed in enumerate(feeds):
            has_interactions = user_id is not None
            chunk_size = len(engagement_keys) + (len(interaction_keys) if has_interactions else 0)
            start = index * chunk_size

            metrics = results[start : start + len(engagement_keys)]
            engagement = {key: value for key, value in zip(engagement_keys, metrics) if value > 0}

            if has_interactions:
                interactions: list[bool] = results[start + len(engagement_keys) : start + chunk_size]
                engagement.update({interaction_key: True for interaction_key, interacted in zip(interaction_keys, interactions) if interacted})

            feed["engagement"] = engagement

        # Fetch author profiles
        author_ids = {feed["author_id"] for feed in feeds}
        keys = ["id", "name", "username", "avatar_url"]

        async with self.cache_redis.pipeline() as pipe:
            for aid in author_ids:
                pipe.hmget(f"users:{aid}:profile", keys)
            profiles = await pipe.execute()

        author_profiles = {profile[0]: dict(zip(keys, profile)) for profile in profiles if profile and profile[0]}

        for feed in feeds:
            feed["author"] = author_profiles.get(feed.pop("author_id"), {})

        return feeds

    async def _check_followers_batch(self, user_id: str, author_ids: list[str]) -> dict[str, bool]:
        """Batch check if user follows multiple authors"""
        async with self.cache_redis.pipeline() as pipe:
            for author_id in author_ids:
                pipe.sismember(f"users:{author_id}:followers", user_id)
            results = await pipe.execute()
        return {aid: result for aid, result in zip(author_ids, results)}


chat_cache_manager = ChatCacheManager(cache_redis=my_cache_redis, search_redis=my_search_redis)
cache_manager = CacheManager(cache_redis=my_cache_redis, search_redis=my_search_redis)
pubsub_manager = RedisPubSubManager(cache_redis=my_cache_redis)
