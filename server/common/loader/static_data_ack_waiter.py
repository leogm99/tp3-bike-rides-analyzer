import logging
from threading import Thread

from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'static_data_ack'


class StaticDataAckWaiter(Thread):
    def __init__(self, rabbit_hostname: str, needed_ack):
        super().__init__()
        self._rabbit_connection = RabbitBlockingConnection(
            rabbit_hostname
        )
        self._static_data_ack = RabbitQueue(
            self._rabbit_connection,
            queue_name='static_data_ack'
        )
        self._needed_ack = needed_ack
        self._ack_count = 0

    def run(self) -> None:
        self._static_data_ack.consume(self.ack_receiver, lambda _: None)
        self._rabbit_connection.start_consuming()

    def ack_receiver(self, _message, _delivery_tag):
        self._ack_count += 1
        logging.info(f'got {self._ack_count} acks')
        if self._needed_ack == self._ack_count:
            self._rabbit_connection.close()
