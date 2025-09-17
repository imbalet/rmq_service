# RMQ service

Сервис - обертка над асинхронной библиотекой `aio-pika` для использования RabbitMQ.
Создавался для удобной работы с RabbitMQ в моих проектах. 

Функционал:
- Отправка сообщений в указанный или стандартный обменник 
- Получение сообщений из очереди с возможностью привязки к обменнику
- Привязка DLQ и DLX к очереди
- Обработка сообщений при помощи колбека.

---

- [RMQ service](#rmq-service)
- [Классы](#классы)
  - [Схемы данных](#схемы-данных)
  - [Сервисы](#сервисы)
    - [ProduceService](#produceservice)
    - [ConsumeService](#consumeservice)
- [Использование](#использование)
    - [Создание channel pool](#создание-channel-pool)
  - [Настройка обменника](#настройка-обменника)
  - [Настройка очереди](#настройка-очереди)
  - [Отправка сообщений](#отправка-сообщений)
  - [Колбек](#колбек)
  - [Получение сообщений](#получение-сообщений)
- [Установка](#установка)
  - [pip](#pip)
  - [Poetry](#poetry)
  - [uv](#uv)



# Классы

## Схемы данных

Класс для настройки параметров очереди

```python
@dataclass
class QueueConfig:
    name: str
    durable: bool = True
```
`name`: str  
Имя очереди  
`durable`: bool  
Параметр для сохранения сообщений при остановке или перезапуске брокера  


```python
@dataclass
class ExchangeConfig:
    name: str
    type: ExchangeType
    durable: bool = True
```
`name`: str  
Имя обменника  
`type`: aio_pika.ExchangeType  
Тип обменника (из типов aio_pika)  
`durable`: bool  
Параметр для сохранения обменника при остановке или перезапуске брокера  


```python
@dataclass
class Message:
    body: bytes
    content_type: str = "text/plain"
    content_encoding: str = "utf-8"
    delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT
    
    def from_text(cls, text: str) -> "Message"
    def from_json(cls, data: dict) -> "Message"
    def to_aio_pika(self) -> aio_pika.Message
```

`body`: bytes  
Тело сообщения  

`content_type`: str  
Тип контента, поддерживаются все типы из aio-pika (`text/plain`, `application/json` и другие)  

`content_encoding`: str  
Кодировка сообщения  

`delivery_mode`: aio_pika.DeliveryMode  
Режим доставки сообщения:
- PERSISTENT (с сохранением на диск)
- NOT_PERSISTENT (без сохранения на диск)  

`from_text(cls, text: str) -> "Message"` classmethod  
Создание сообщения из текста  

`from_json(cls, data: dict) -> "Message"` classmethod  
Создание сообщения из словаря, передача как json  

`to_aio_pika(self) -> aio_pika.Message`  
Конвертация сообщения в тип aio-pika



## Сервисы

### ProduceService

```python
class ProduceService:
    def __init__(
        self,
        channel_pool: Pool[Channel],
        routing_key: str,
        exchange_config: ExchangeConfig | None = None,
    )
```

`channel_pool`: aio_pika.pool.Pool - Пул каналов  
`routing_key`: str - routing key для отправки сообщений в очередь  
`exchange_config`: ExchangeConfig - конфигурация обменника (None, если используется стандартный обменник)  


```python
async def setup(self)
```

Метод для настройки обменника.


```python
async def produce(self, message: Message)
```

Отправка сообщения
`message`: Message - Сообщение

### ConsumeService

```python
class ConsumeService:
    def __init__(
        self,
        channel_pool: Pool[Channel],
        queue_config: QueueConfig,
        exchange_config: ExchangeConfig | None = None,
        dlq: QueueConfig | None = None,
        dlx: ExchangeConfig | None = None,
        prefetch_count: int = 10,
    )
```
`channel_pool`: aio_pika.pool.Pool  
Пул каналов  

`queue_config`: QueueConfig  
Конфигурация очереди  

`exchange_config`: ExchangeConfig | None  
Конфигурация обменника для привязки очереди (опционально).  

`dlq`: QueueConfig | None  
Конфигурации dead letter queue (опционально), требует настройки dead letter exchange (dlx).  

`dlx`: ExchangeConfig | None  
Конфигурация dead letter exchange для dlq (опционально).  

`prefetch_count`: int  
Лимит сообщений на одновременную обработку, сейчас ни на что не влияет из-за последовательной обработки сообщений.


```python
async def setup(self)
```

Метод для настройки очереди, обменника, dlq и dlx.

```python
async def consume(
    self,
    callback: MessageCallback,
    retries: int = 3,
    **kwargs,
)
```
Метод для получения сообщений из очереди.

`callback`: MessageCallback  
Пользовательская функция-колбек.  

`retries`: int  
Число попыток обработки сообщения перед nack.  

`**kwargs`
Именованные аргументы, которые будут переданы в функцию колбек (для работы в колбеке с внешними зависимостями). Можно использовать `functools.partial` как более чистый способ.


# Использование

На данный момент поддерживается работа только с **channel pool**. Необходимо создать пул подключений и каналов и передать channel pool в конструктор Produce или Consume сервиса.

### Создание channel pool

Пример создания пула каналов из документации aio-pika. Далее в примерах опущено подключение и создание пула.

```python
import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool


async def main() -> None:
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust("amqp://guest:guest@localhost/")

    connection_pool: Pool = Pool(get_connection, max_size=2)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool: Pool[aio_pika.Channel] = Pool(get_channel, max_size=10)


if __name__ == "__main__":
    asyncio.run(main())
```


## Настройка обменника

Поддерживаются все типы обменников из aio-pika
```python
from aio_pika import ExchangeType

from rmq_service import ExchangeConfig

exchange_config = ExchangeConfig(
    name="name",                # Имя обменника
    type=ExchangeType.DIRECT,   # Тип обменника
    durable=True,               # durable
)
```

## Настройка очереди

```python
from rmq_service import QueueConfig

queue_config = QueueConfig(
    name="name",                # Имя очереди
    durable=True,               # durable
)
```


## Отправка сообщений

Пример отправки сообщения в стандартный обменник по routing key. 
Также поддерживается отправка в ранее настроенный обменник, если передать его в конструктор аргументом `exchange_config`

```python
import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool

from rmq_service import Message, ProduceService


async def main() -> None:
    # Connecting and creating channel pool

    service = ProduceService(channel_pool=channel_pool, routing_key="queue_name")
    await service.setup()
    await service.produce(message=Message(body=b"body"))


if __name__ == "__main__":
    asyncio.run(main())
```

## Колбек

Пользовательская функция-колбек может принимать следующие параметры
```python
async def callback(
    data: bytes,
    message: aio_pika.IncomingMessage,
    headers: dict,
    routing_key: str,
    exchange: str,
    **kwargs,
)
```
`data`: bytes  
Тело сообщения.

`message`: aio_pika.IncomingMessage  
Сырой объект сообщения.

`headers`: dict  
Заголовки сообщения.

`routing_key`: str  
Название routing key сообщения.

`exchange`: str  
Название exchange сообщения.

`**kwargs`  
Именованные аргументы переданные при вызове функции `consume`.


Можно объявить только необходимые аргументы и `**kwargs`, например:  
```python
async def callback(
    data: bytes,
    headers: dict,
    **kwargs,
)
```


Есть возможность использовать `functools.partial` для более удобной передачи пользовательских объектов и зависимостей в колбек.

```python
import asyncio
from functools import partial

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool

from rmq_service import ConsumeService, ExchangeConfig, QueueConfig


async def callback(
    my_obj,
    data: bytes,
    message: aio_pika.IncomingMessage,
    headers: dict,
    routing_key: str,
    exchange: str,
    **kwargs,
):
    pass


class MyObject:
    pass


async def main() -> None:
    # Connecting and creating channel pool

    exchange_config = ExchangeConfig(
        name="exchange.name", type=aio_pika.ExchangeType.DIRECT, durable=True
    )

    queue_config = QueueConfig(name="name", durable=True)

    service = ConsumeService(
        channel_pool=channel_pool,
        queue_config=queue_config,
        exchange_config=exchange_config,
    )
    await service.setup()
    await service.consume(callback=partial(callback, MyObject()))


if __name__ == "__main__":
    asyncio.run(main())
```


## Получение сообщений



Пример блокирующего получения сообщения из очереди без настройки DLQ и DLX. Для их настройки нужно передать конфигурации в конструктор и все будет настроено при вызове метода `setup`. 

```python
import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool

from rmq_service import ConsumeService, ExchangeConfig, QueueConfig


async def callback(
    my_obj,
    data: bytes,
    message: aio_pika.IncomingMessage,
    headers: dict,
    routing_key: str,
    exchange: str,
    **kwargs,
):
    pass


async def main() -> None:
    # Connecting and creating channel pool

    exchange_config = ExchangeConfig(
        name="exchange.name", type=aio_pika.ExchangeType.DIRECT, durable=True
    )

    queue_config = QueueConfig(name="name", durable=True)

    service = ConsumeService(
        channel_pool=channel_pool,
        queue_config=queue_config,
        exchange_config=exchange_config,
    )
    await service.setup()
    await service.consume(callback=callback)


if __name__ == "__main__":
    asyncio.run(main())

```


Пример неблокирующего получения сообщения из очереди. При `cancel` задача будет корректно завершена. 

```python
import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool

from rmq_service import ConsumeService, ExchangeConfig, QueueConfig


async def callback(
    my_obj,
    data: bytes,
    message: aio_pika.IncomingMessage,
    headers: dict,
    routing_key: str,
    exchange: str,
    **kwargs,
):
    pass


async def main() -> None:
    # Connecting and creating channel pool

    exchange_config = ExchangeConfig(
        name="exchange.name", type=aio_pika.ExchangeType.DIRECT, durable=True
    )

    queue_config = QueueConfig(name="name", durable=True)

    service = ConsumeService(
        channel_pool=channel_pool,
        queue_config=queue_config,
        exchange_config=exchange_config,
    )
    await service.setup()
    task = asyncio.create_task(service.consume(callback=callback))

    await asyncio.sleep(10)  # do something

    task.cancel()
    await task


if __name__ == "__main__":
    asyncio.run(main())
```


# Установка

## pip

Для установки необходимо выполнить

```bash
pip install git+https://github.com/imbalet/rmq_service
```

## Poetry

Для установки необходимо выполнить

```bash
poetry add git+https://github.com/imbalet/rmq_service
```


## uv

Для установки необходимо выполнить

```bash
uv add git+https://github.com/imbalet/rmq_service
```