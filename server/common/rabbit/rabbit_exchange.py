from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection


class RabbitExchange:
    def __init__(self, rabbit_connection: RabbitBlockingConnection,
                 exchange_name: str = '',
                 exchange_type: str = ''):
        self._rabbit_connection = rabbit_connection
        if exchange_name != '':
            self._rabbit_connection.exchange_declare(exchange_name, exchange_type)
        self._exchange_name = exchange_name

    def publish(self, message: str, routing_key: str = ''):
        self._rabbit_connection.publish(
            message=message,
            exchange_name=self._exchange_name,
            routing_key=routing_key,
        )
