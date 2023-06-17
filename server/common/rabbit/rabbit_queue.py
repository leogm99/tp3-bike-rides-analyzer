from common_utils.protocol.protocol import Protocol
from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common_utils.protocol.message import CLIENT_ID


class RabbitQueue:
    def __init__(self,
                 rabbit_connection: RabbitBlockingConnection,
                 queue_name: str = '',
                 bind_exchange: str = '',
                 bind_exchange_type: str = '',
                 routing_key: str = '',
                 producers: int = 1):
        self._producers = producers
        self._count_eof = {}
        self._rabbit_connection = rabbit_connection
        self._queue_name = rabbit_connection.queue_declare(queue_name)
        self._consumer_tag = None
        if bind_exchange != '':
            rabbit_connection.exchange_declare(exchange_name=bind_exchange,
                                               exchange_type=bind_exchange_type)
            rabbit_connection.queue_bind(queue_name=self._queue_name,
                                         exchange_name=bind_exchange,
                                         routing_key=routing_key)

    def consume(self, on_message_callback, on_producer_finished, auto_ack=False):
        def wrap_on_message_callback(ch, method, properties, body):
            message = Protocol.deserialize_message(body)
            delivery_tag = method.delivery_tag
            self.ack(delivery_tag)
            if message.is_eof():
                client_id = message.client_id
                self._count_eof[client_id] = self._count_eof.get(client_id, 0) + 1
                if self._count_eof[client_id] == self._producers:
                    return on_producer_finished(message, delivery_tag)
                if self._count_eof[client_id] > self._producers:
                    raise ValueError(f'Received {self._count_eof[client_id]}, expected {self._producers}')
            else:
                return on_message_callback(message, delivery_tag)
        self._consumer_tag = self._rabbit_connection.consume(
            queue_name=self._queue_name,
            on_message_callback=wrap_on_message_callback,
            auto_ack=auto_ack,
        )

    def cancel(self):
        if self._consumer_tag is not None:
            self._rabbit_connection.cancel_consumer(self._consumer_tag)

    def ack(self, delivery_tag):
        self._rabbit_connection.ack(delivery_tag=delivery_tag)


