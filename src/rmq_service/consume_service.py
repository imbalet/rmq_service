import asyncio
import logging

from aio_pika import Channel, ExchangeType
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aio_pika.pool import Pool

from .schemas import ExchangeConfig, MessageCallback, QueueConfig

logger = logging.getLogger(__name__)


class ConsumeService:
    def __init__(
        self,
        channel_pool: Pool[Channel],
        queue_config: QueueConfig,
        exchange_config: ExchangeConfig | None = None,
        dlq: QueueConfig | None = None,
        dlx: ExchangeConfig | None = None,
        prefetch_count: int = 10,
    ):
        self.channel_pool = channel_pool
        self.prefetch_count = prefetch_count

        self.queue_config = queue_config
        self.exchange_config = exchange_config
        self.dlq = dlq
        self.dlx = dlx

        self.queue: AbstractQueue | None = None

        self._stopped = asyncio.Event()

    async def _setup_dead_lettering(self, channel: Channel):
        dlq = None
        dlx = None
        if self.dlx:
            dlx = await channel.declare_exchange(
                name=self.dlx.name, type=self.dlx.type, durable=self.dlx.durable
            )
            if self.dlq:
                dlq = await channel.declare_queue(
                    name=self.dlq.name, durable=self.dlq.durable
                )
                routing_key = (
                    "" if self.dlx.type == ExchangeType.FANOUT else self.dlq.name
                )
                await dlq.bind(exchange=dlx, routing_key=routing_key)

    async def _setup_queue(self, channel: Channel):
        queue_args = {}
        if self.dlx:
            queue_args["x-dead-letter-exchange"] = self.dlx.name
            if self.dlq:
                queue_args["x-dead-letter-routing-key"] = self.dlq.name

        queue = await channel.declare_queue(
            name=self.queue_config.name,
            durable=self.queue_config.durable,
            arguments=queue_args,
        )
        if self.exchange_config:
            exchange = await channel.declare_exchange(
                name=self.exchange_config.name,
                type=self.exchange_config.type,
                durable=self.exchange_config.durable,
            )
            routing_key = (
                ""
                if self.exchange_config.type == ExchangeType.FANOUT
                else self.queue_config.name
            )
            await queue.bind(exchange=exchange, routing_key=routing_key)
        return queue

    async def _handle_message(
        self,
        message: AbstractIncomingMessage,
        callback: MessageCallback,
        **kwargs,
    ):
        data_map = {
            "data": message.body,
            "message": message,
            "headers": dict(message.headers) if message.headers else {},
            "routing_key": message.routing_key,
            "exchange": message.exchange,
        }

        await callback(**data_map, **kwargs)

    async def setup(self):
        try:
            async with self.channel_pool.acquire() as channel:
                await self._setup_dead_lettering(channel)
                self.queue = await self._setup_queue(channel)
        except asyncio.CancelledError:
            logger.info("Setup task cancelled")
        except Exception:
            logger.critical("Unexpected setup error", exc_info=True)
            raise

    async def consume(
        self,
        callback: MessageCallback,
        retries: int = 3,
        **kwargs,
    ):
        if not self.queue:
            logger.critical("Queue is not initialized")
            raise ValueError("Queue is not initialized")
        try:
            async with self.channel_pool.acquire() as channel:
                await channel.set_qos(prefetch_count=self.prefetch_count)
                async with self.queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        if self._stopped.is_set():
                            break
                        for attempt in range(retries):
                            try:
                                await self._handle_message(message, callback, **kwargs)
                                await message.ack()
                                break
                            except Exception as e:
                                logger.warning(
                                    "Retry %d failed", attempt + 1, exc_info=True
                                )
                                if attempt + 1 >= retries:
                                    await message.nack(requeue=False)
                                    logger.warning(
                                        "Message failed after %d retries with error %s",
                                        attempt + 1,
                                        e,
                                        exc_info=True,
                                    )

        except asyncio.CancelledError:
            logger.info("Consumer stopped gracefully")
        except Exception:
            logger.critical("Consume task stopped unexpectedly", exc_info=True)
            raise

    def stop(self):
        self._stopped.set()
