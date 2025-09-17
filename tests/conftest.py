from unittest.mock import AsyncMock

import pytest
from aio_pika import Channel, Exchange, ExchangeType, IncomingMessage, Queue
from aio_pika.pool import Pool

from rmq_service import (
    ExchangeConfig,
    Message,
    ProduceService,
)


@pytest.fixture
def channel_mock(exchange_mock, queue_mock):
    mock = AsyncMock(spec=Channel)
    mock.declare_exchange.return_value = exchange_mock
    mock.declare_queue.return_value = queue_mock
    mock.default_exchange = exchange_mock
    return mock


@pytest.fixture
def channel_pool_mock(channel_mock, exchange_mock):
    mock = AsyncMock(spec=Pool[Channel])

    class AcquireCM:
        async def __aenter__(self):
            return channel_mock

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

    mock.acquire.return_value = AcquireCM()

    return mock


@pytest.fixture
def exchange_mock():
    mock = AsyncMock(spec=Exchange)
    return mock


@pytest.fixture
def queue_mock(incoming_message):
    mock = AsyncMock(spec=Queue)

    async def fake_iter():
        for msg in [incoming_message, incoming_message]:
            yield msg

    cm = AsyncMock()
    cm.__aenter__.return_value = fake_iter()
    cm.__aexit__.return_value = False

    mock.iterator.return_value = cm
    return mock


@pytest.fixture
def incoming_message():
    mock = AsyncMock(spec=IncomingMessage)
    mock.body = b"body"
    mock.headers = {"header": "value"}
    mock.routing_key = "routing_key"
    mock.exchange = "exchange"
    return mock


@pytest.fixture
def routing_key():
    return "routing_key"


@pytest.fixture
def message():
    return Message(body=b"body", content_type="text/plain")


@pytest.fixture
def exchange_config():
    return ExchangeConfig(name="name", type=ExchangeType.DIRECT)


@pytest.fixture
def produce_service(channel_pool_mock, routing_key, exchange_config):
    return ProduceService(
        channel_pool=channel_pool_mock,
        routing_key=routing_key,
        exchange_config=exchange_config,
    )


@pytest.fixture
def produce_service_without_exchange(channel_pool_mock, routing_key):
    return ProduceService(channel_pool=channel_pool_mock, routing_key=routing_key)
