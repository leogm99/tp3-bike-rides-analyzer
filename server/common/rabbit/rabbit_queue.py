from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection


class RabbitQueue:
    def __init__(self,
                 rabbit_connection: RabbitBlockingConnection,
                 queue_name: str = '',
                 bind_exchange: str = '',
                 bind_exchange_type: str = '',
                 routing_key: str = ''):
        self._rabbit_connection = rabbit_connection
        self._queue_name = rabbit_connection.queue_declare(queue_name)
        if bind_exchange != '':
            rabbit_connection.exchange_declare(exchange_name=bind_exchange,
                                               exchange_type=bind_exchange_type)
            rabbit_connection.queue_bind(queue_name=self._queue_name,
                                         exchange_name=bind_exchange,
                                         routing_key=routing_key)

    def consume(self, on_message_callback):
        def wrap_on_message_callback(ch, method, properties, body):
            return on_message_callback(body)
        self._rabbit_connection.consume(
            queue_name=self._queue_name,
            on_message_callback=wrap_on_message_callback
        )


