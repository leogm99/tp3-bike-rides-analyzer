import json

from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection


class RabbitQueue:
    def __init__(self,
                 rabbit_connection: RabbitBlockingConnection,
                 queue_name: str = '',
                 bind_exchange: str = '',
                 bind_exchange_type: str = '',
                 routing_key: str = '',
                 producers: int = 1):
        self._producers = producers
        self._count_eof = 0
        self._rabbit_connection = rabbit_connection
        self._queue_name = rabbit_connection.queue_declare(queue_name)
        self._consumer_tag = None
        if bind_exchange != '':
            rabbit_connection.exchange_declare(exchange_name=bind_exchange,
                                               exchange_type=bind_exchange_type)
            rabbit_connection.queue_bind(queue_name=self._queue_name,
                                         exchange_name=bind_exchange,
                                         routing_key=routing_key)

    def consume(self, on_message_callback, on_producer_finished, auto_ack=True):
        def wrap_on_message_callback(ch, method, properties, body):
            message_obj = json.loads(body)
            delivery_tag = method.delivery_tag
            if message_obj['payload'] == 'EOF':
                self._count_eof += 1
                if self._count_eof == self._producers:
                    return on_producer_finished(message_obj, delivery_tag)
            return on_message_callback(message_obj, delivery_tag)
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


