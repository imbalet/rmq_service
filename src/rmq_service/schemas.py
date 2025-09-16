import json
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import aio_pika
from aio_pika import DeliveryMode, ExchangeType


@runtime_checkable
class MessageCallback(Protocol):
    async def __call__(self, *args: Any, **kwargs: Any) -> None: ...


@dataclass
class QueueConfig:
    name: str
    durable: bool = True


@dataclass
class ExchangeConfig:
    name: str
    type: ExchangeType
    durable: bool = True


@dataclass
class Message:
    body: bytes
    content_type: str = "text/plain"
    content_encoding: str = "utf-8"
    delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT

    @classmethod
    def from_text(cls, text: str) -> "Message":
        return cls(body=text.encode(cls.content_encoding), content_type="text/plain")

    @classmethod
    def from_json(cls, data: dict) -> "Message":
        return cls(
            body=json.dumps(data).encode("utf-8"), content_type="application/json"
        )

    def to_aio_pika(self) -> aio_pika.Message:
        return aio_pika.Message(
            body=self.body,
            content_type=self.content_type,
            content_encoding=self.content_encoding,
            delivery_mode=self.delivery_mode,
        )
