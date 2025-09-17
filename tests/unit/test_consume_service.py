import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from aio_pika import ExchangeType

from rmq_service import ConsumeService, ExchangeConfig, QueueConfig


def get_consume_service(
    channel_pool_mock,
    add_exchange: bool = False,
    add_dlq: bool = False,
    add_dlx: bool = False,
):
    exchange = None
    dlq = None
    dlx = None
    if add_exchange:
        exchange = ExchangeConfig(name="name", type=ExchangeType.DIRECT)

    if add_dlq:
        dlq = QueueConfig(name="name")

    if add_dlx:
        dlx = ExchangeConfig(name="name", type=ExchangeType.DIRECT)

    queue = QueueConfig(name="name")

    return ConsumeService(
        channel_pool=channel_pool_mock,
        queue_config=queue,
        exchange_config=exchange,
        dlq=dlq,
        dlx=dlx,
    )


async def test_setup_dead_lettering_dlq_dlx(
    channel_pool_mock, channel_mock, queue_mock
):
    service = get_consume_service(channel_pool_mock, add_dlq=True, add_dlx=True)
    await service._setup_dead_lettering(channel_mock)

    assert service.dlq and service.dlx
    channel_mock.declare_exchange.assert_awaited_once_with(
        name=service.dlx.name, type=service.dlx.type, durable=service.dlx.durable
    )
    channel_mock.declare_queue.assert_awaited_once_with(
        name=service.dlq.name, durable=service.dlq.durable
    )
    queue_mock.bind.assert_awaited_once()
    assert queue_mock.bind.await_args[1]["routing_key"] == service.dlq.name


async def test_setup_dead_lettering_dlx(channel_pool_mock, channel_mock, queue_mock):
    service = get_consume_service(channel_pool_mock, add_dlx=True)
    await service._setup_dead_lettering(channel_mock)

    assert service.dlx
    channel_mock.declare_exchange.assert_awaited_once_with(
        name=service.dlx.name, type=service.dlx.type, durable=service.dlx.durable
    )
    channel_mock.declare_queue.assert_not_awaited()
    queue_mock.bind.assert_not_awaited()


async def test_setup_dead_lettering_empty(channel_pool_mock, channel_mock, queue_mock):
    service = get_consume_service(channel_pool_mock)
    await service._setup_dead_lettering(channel_mock)

    channel_mock.declare_exchange.assert_not_awaited()
    channel_mock.declare_queue.assert_not_awaited()
    queue_mock.bind.assert_not_awaited()


async def test_setup_queue_dlx(channel_pool_mock, channel_mock):
    service = get_consume_service(channel_pool_mock, add_dlx=True)
    await service._setup_queue(channel_mock)

    assert service.dlx
    channel_mock.declare_queue.assert_awaited_once_with(
        name=service.queue_config.name,
        durable=service.queue_config.durable,
        arguments={"x-dead-letter-exchange": service.dlx.name},
    )


async def test_setup_queue_dlx_dlq(channel_pool_mock, channel_mock):
    service = get_consume_service(channel_pool_mock, add_dlq=True, add_dlx=True)
    await service._setup_queue(channel_mock)

    assert service.dlq and service.dlx
    channel_mock.declare_queue.assert_awaited_once_with(
        name=service.queue_config.name,
        durable=service.queue_config.durable,
        arguments={
            "x-dead-letter-routing-key": service.dlq.name,
            "x-dead-letter-exchange": service.dlx.name,
        },
    )


async def test_setup_queue_exchange(channel_pool_mock, channel_mock, queue_mock):
    service = get_consume_service(channel_pool_mock, add_exchange=True)
    res = await service._setup_queue(channel_mock)

    assert res is not None
    assert service.exchange_config
    channel_mock.declare_exchange.assert_awaited_once_with(
        name=service.exchange_config.name,
        type=service.exchange_config.type,
        durable=service.exchange_config.durable,
    )
    assert queue_mock.bind.await_args[1]["routing_key"] == service.queue_config.name


async def test_setup_queue_no_exchange(channel_pool_mock, channel_mock, queue_mock):
    service = get_consume_service(channel_pool_mock)
    res = await service._setup_queue(channel_mock)

    assert res is not None
    channel_mock.declare_exchange.assert_not_awaited()
    queue_mock.bind.assert_not_awaited()


@patch.object(ConsumeService, "_setup_dead_lettering", new_callable=AsyncMock)
@patch.object(ConsumeService, "_setup_queue", new_callable=AsyncMock)
async def test_setup_valid(
    _setup_dead_lettering_mock, _setup_queue_mock, channel_pool_mock
):
    service = get_consume_service(channel_pool_mock)
    await service.setup()

    _setup_dead_lettering_mock.assert_awaited_once()
    _setup_queue_mock.assert_awaited_once()


@patch.object(ConsumeService, "_setup_dead_lettering", new_callable=AsyncMock)
@patch.object(ConsumeService, "_setup_queue", new_callable=AsyncMock)
async def test_setup_error_dead(
    _setup_dead_lettering_mock, _setup_queue_mock, channel_pool_mock
):
    _setup_dead_lettering_mock.side_effect = ValueError()

    service = get_consume_service(channel_pool_mock)
    with pytest.raises(ValueError):
        await service.setup()

    _setup_dead_lettering_mock.assert_awaited_once()


@patch.object(ConsumeService, "_setup_dead_lettering", new_callable=AsyncMock)
@patch.object(ConsumeService, "_setup_queue", new_callable=AsyncMock)
async def test_setup_error_queue(
    _setup_dead_lettering_mock, _setup_queue_mock, channel_pool_mock
):
    _setup_queue_mock.side_effect = ValueError()

    service = get_consume_service(channel_pool_mock)
    with pytest.raises(ValueError):
        await service.setup()

    _setup_queue_mock.assert_awaited_once()


async def test_handle_message(channel_pool_mock, incoming_message):
    service = get_consume_service(channel_pool_mock)

    async def callback(data: bytes, my_kwarg: Any, **kwargs: Any) -> None:
        assert my_kwarg == "my"
        assert data == incoming_message.body

    await service._handle_message(
        message=incoming_message,
        callback=callback,
        my_kwarg="my",
    )


async def test_consume_with_custom_kwarg(channel_pool_mock, incoming_message):
    service = get_consume_service(channel_pool_mock)

    async def callback(data: bytes, my_kwarg: Any, **kwargs: Any) -> None:
        assert data == incoming_message.body

    await service.setup()
    await service.consume(callback=callback, my_kwarg="my")

    incoming_message.ack.assert_awaited()
    incoming_message.nack.assert_not_awaited()


async def test_consume(channel_pool_mock, incoming_message):
    service = get_consume_service(channel_pool_mock)

    async def callback(data: bytes, **kwargs: Any) -> None:
        assert data == incoming_message.body

    await service.setup()
    await service.consume(callback=callback)

    incoming_message.ack.assert_awaited()
    incoming_message.nack.assert_not_awaited()


async def test_consume_fail_processing(channel_pool_mock, incoming_message):
    service = get_consume_service(channel_pool_mock)

    async def callback(data: bytes, **kwargs: Any) -> None:
        raise ValueError()

    await service.setup()
    await service.consume(callback=callback)

    incoming_message.ack.assert_not_awaited()
    incoming_message.nack.assert_awaited()


async def test_consume_retries_valid(channel_pool_mock, incoming_message):
    service = get_consume_service(channel_pool_mock)

    attempt = 0

    async def callback(data: bytes, **kwargs: Any) -> None:
        nonlocal attempt
        if attempt == 2:
            pass
        else:
            attempt += 1
            raise ValueError()

    await service.setup()
    await service.consume(callback=callback, retries=3)

    incoming_message.ack.assert_awaited()
    incoming_message.nack.assert_not_awaited()


async def test_consume_cancel(channel_pool_mock, incoming_message, queue_mock):
    service = get_consume_service(channel_pool_mock)

    async def callback(data: bytes, **kwargs: Any) -> None:
        pass

    async def fake_iter():
        for msg in [incoming_message]:
            yield msg
            await asyncio.Event().wait()

    queue_mock.iterator.return_value.__aenter__.return_value = fake_iter()

    await service.setup()
    task = asyncio.create_task(service.consume(callback=callback))

    await asyncio.sleep(0.1)

    res = task.cancel()

    await asyncio.wait([task], timeout=1)

    assert res
    assert task.done()
    assert task.exception() is None


async def test_consume_error_task(channel_pool_mock, incoming_message, queue_mock):
    service = get_consume_service(channel_pool_mock)

    async def callback(data: bytes, **kwargs: Any) -> None:
        pass

    queue_mock.iterator.return_value.__aenter__.side_effect = ValueError()

    await service.setup()
    task = asyncio.create_task(service.consume(callback=callback))

    await asyncio.sleep(0.1)

    assert task.done()
    assert isinstance(task.exception(), ValueError)


async def test_consume_error_await(channel_pool_mock, incoming_message, queue_mock):
    service = get_consume_service(channel_pool_mock)

    async def callback(data: bytes, **kwargs: Any) -> None:
        pass

    queue_mock.iterator.return_value.__aenter__.side_effect = ValueError()

    await service.setup()
    with pytest.raises(ValueError):
        await service.consume(callback=callback)
