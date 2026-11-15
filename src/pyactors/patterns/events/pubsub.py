"""Pub/Sub actor: topic-based subscriptions."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Hashable
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.actor_ref import ActorRef
from pyactors.patterns.messages import GetStats

__all__ = ["PubSub", "Publish", "Subscribe", "Unsubscribe"]


@dataclass(frozen=True, slots=True)
class Subscribe:
    topic: Hashable
    subscriber: ActorRef[Any, Any]


@dataclass(frozen=True, slots=True)
class Unsubscribe:
    topic: Hashable
    subscriber: ActorRef[Any, Any]


@dataclass(frozen=True, slots=True)
class Publish:
    topic: Hashable
    message: Any


class PubSub(Actor[Any, Any]):
    """In-process publish/subscribe hub.

    ``Publish`` delivers via ``tell`` to each subscriber (fire-and-forget).
    Dead subscribers that raise on ``tell`` are removed.  At-most-once;
    subscribers that are overloaded may raise ``MailboxFull`` and be dropped.
    """

    def __init__(self) -> None:
        self._topics: dict[Hashable, list[ActorRef[Any, Any]]] = defaultdict(list)
        self._published = 0
        self._delivered = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "topics": {t: len(s) for t, s in self._topics.items()},
                "published": self._published,
                "delivered": self._delivered,
            }
        if isinstance(message, Subscribe):
            subs = self._topics[message.topic]
            if message.subscriber not in subs:
                subs.append(message.subscriber)
            return len(subs)
        if isinstance(message, Unsubscribe):
            existing = self._topics.get(message.topic)
            if not existing:
                return False
            try:
                existing.remove(message.subscriber)
            except ValueError:
                return False
            if not existing:
                del self._topics[message.topic]
            return True
        if isinstance(message, Publish):
            return self._publish(message.topic, message.message)
        raise TypeError(f"unsupported pubsub message: {type(message)!r}")

    def _publish(self, topic: Hashable, payload: Any) -> int:
        self._published += 1
        subs = list(self._topics.get(topic, ()))
        alive: list[ActorRef[Any, Any]] = []
        delivered = 0
        for sub in subs:
            try:
                sub.tell(payload)
            except Exception:
                continue
            alive.append(sub)
            delivered += 1
        if topic in self._topics:
            if alive:
                self._topics[topic] = alive
            else:
                del self._topics[topic]
        self._delivered += delivered
        return delivered
