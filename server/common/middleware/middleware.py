from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue


class Middleware:
    def __init__(self, hostname: str):
        self._rabbit_connection = RabbitBlockingConnection(
            rabbit_hostname=hostname,
        )

    def start(self):
        self._rabbit_connection.start_consuming()

    @classmethod
    def receive(cls, queue: RabbitQueue, on_message_callback, on_end_message_callback, auto_ack=False):
        queue.consume(on_message_callback=on_message_callback,
                      on_producer_finished=on_end_message_callback,
                      auto_ack=auto_ack)

    @classmethod
    def send(cls, message, exchange: RabbitExchange, routing_key: str = ''):
        exchange.publish(message, routing_key)

    def stop(self):
        self._rabbit_connection.close()

