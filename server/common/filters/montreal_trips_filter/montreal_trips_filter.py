import json
from common.dag_node import DAGNode
from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue

CITY = 'montreal'


class MontrealTripsFilter(DAGNode):
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
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message):
        message_obj = json.loads(message)
        payload = message_obj['payload']
        if not isinstance(payload, str):
            trip_city = payload['city']
            if trip_city == CITY:
                # TODO
                pass
        pass

    def close(self):
        pass
