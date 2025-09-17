import pytest


async def test_setup_with_exchange_valid(
    produce_service, channel_mock, exchange_config
):
    await produce_service.setup()

    assert produce_service.exchange is not None
    channel_mock.declare_exchange.assert_awaited_once_with(
        name=exchange_config.name,
        type=exchange_config.type,
        durable=exchange_config.durable,
    )


async def test_setup_with_exchange_error(produce_service, channel_mock):
    channel_mock.declare_exchange.side_effect = ValueError()
    with pytest.raises(ValueError):
        await produce_service.setup()

    assert produce_service.exchange is None


async def test_setup_without_exchange_valid(
    produce_service_without_exchange, channel_mock
):
    await produce_service_without_exchange.setup()

    assert produce_service_without_exchange.exchange is None
    channel_mock.declare_exchange.assert_not_awaited()


async def test_produce_with_exchange_valid(produce_service, exchange_mock, message):
    await produce_service.setup()
    await produce_service.produce(message=message)

    exchange_mock.publish.assert_awaited_once()


async def test_produce_with_exchange_error(produce_service, exchange_mock, message):
    exchange_mock.publish.side_effect = ValueError()

    await produce_service.setup()
    with pytest.raises(ValueError):
        await produce_service.produce(message=message)

    exchange_mock.publish.assert_awaited_once()


async def test_produce_without_exchange_valid(
    produce_service_without_exchange, channel_mock, message
):
    await produce_service_without_exchange.setup()
    await produce_service_without_exchange.produce(message=message)

    channel_mock.default_exchange.publish.assert_awaited_once()


async def test_produce_without_exchange_error(
    produce_service_without_exchange, channel_mock, message
):
    channel_mock.default_exchange.publish.side_effect = ValueError()

    await produce_service_without_exchange.setup()
    with pytest.raises(ValueError):
        await produce_service_without_exchange.produce(message=message)

    channel_mock.default_exchange.publish.assert_awaited_once()
