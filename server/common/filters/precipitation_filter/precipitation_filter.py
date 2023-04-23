import json
import logging

from common.dag_node import DAGNode
from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue


class PrecipitationFilter(DAGNode):
    precipitation_threshold = 30

    def __init__(self, rabbit_hostname: str, queue_name: str):
        super().__init__()
        self._rabbit_connection = RabbitBlockingConnection(
            rabbit_hostname=rabbit_hostname,
        )
        self._input_queue = RabbitQueue(
            rabbit_connection=self._rabbit_connection,
            queue_name=queue_name,
        )

    def run(self):
        self._input_queue.consume(self.on_message_callback)

    def on_message_callback(self, message):
        message_obj = json.loads(message)
        payload = message_obj['payload']
        if not isinstance(payload, str):
            precipitation = float(payload['prectot'])
            if precipitation > self.precipitation_threshold:
                # TODO
                pass

    def close(self):
        if not self.closed:
            self.closed = True
            self._rabbit_connection.close()
