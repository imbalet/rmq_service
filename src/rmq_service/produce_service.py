import asyncio
import logging

from aio_pika import Channel
from aio_pika.abc import AbstractExchange
from aio_pika.pool import Pool

from .schemas import ExchangeConfig, Message

logger = logging.getLogger(__name__)


class ProduceService:
    def __init__(
        self,
        channel_pool: Pool[Channel],
        routing_key: str,
        exchange_config: ExchangeConfig | None = None,
    ):
        self.channel_pool = channel_pool

        self.routing_key = routing_key
        self.exchange_config = exchange_config

        self.exchange: AbstractExchange | None = None

    async def setup(self):
        if not self.exchange_config:
            return
        try:
            async with self.channel_pool.acquire() as channel:
                self.exchange = await channel.declare_exchange(
                    name=self.exchange_config.name,
                    type=self.exchange_config.type,
                    durable=self.exchange_config.durable,
                )

        except asyncio.CancelledError:
            logger.info("Setup task cancelled")
        except Exception:
            logger.critical("Unexpected setup error", exc_info=True)
            raise

    async def produce(self, message: Message):
        try:
            async with self.channel_pool.acquire() as channel:
                aio_pika_message = message.to_aio_pika()

                if self.exchange:
                    await self.exchange.publish(
                        message=aio_pika_message, routing_key=self.routing_key
                    )
                else:
                    await channel.default_exchange.publish(
                        message=aio_pika_message, routing_key=self.routing_key
                    )

        except asyncio.CancelledError:
            logger.info("Producer stopped gracefully")
        except Exception:
            logger.critical("Produce task stopped unexpectedly", exc_info=True)
            raise
