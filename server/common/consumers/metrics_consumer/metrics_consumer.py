import json
import logging

from common.dag_node import DAGNode
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue
from collections import defaultdict

QUEUE_NAME = 'metrics_consumer'
LOADER_ROUTING_KEY = 'metrics_waiter'


class MetricsConsumer(DAGNode):
    def __init__(self,
                 rabbit_hostname: str,
                 producers: int = 1):
        super().__init__(rabbit_hostname)
        logging.info(f'expected EOFS: {producers}')
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=producers
        )
        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )
        self._metrics = defaultdict(list)

    def run(self):
        try:
            self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
            self._rabbit_connection.start_consuming()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: sucess')

    def on_message_callback(self, message, delivery_tag):
        if message['payload'] == 'EOF':
            return
        self._metrics[message['type']].append(message['payload'])

    def on_producer_finished(self, message, delivery_tag):
        self.publish(json.dumps(self._metrics), self._output_exchange, routing_key=LOADER_ROUTING_KEY)
        logging.info('action: publish-to-loader | status: success')
        self.close()

