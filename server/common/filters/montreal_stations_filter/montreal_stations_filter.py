import logging
import json

from common.dag_node import DAGNode
from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue

CITY = 'montreal'


class MontrealStationsFilter(DAGNode):
    def __init__(self, rabbit_hostname: str, queue_name):
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
        obj_message = json.loads(message)
        payload = obj_message['payload']
        if not isinstance(payload, str):
            city = payload['city']
            if city == CITY:
                logging.info(f'action: on-message-callback | status: success | message: {payload}')
                # TODO
                pass
        pass

    def close(self):
        if not self.closed:
            self.closed = True
            self._rabbit_connection.close()
        pass
