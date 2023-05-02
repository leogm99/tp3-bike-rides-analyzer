import json
from typing import Tuple

from common.aggregators.rolling_average_aggregator.rolling_average_aggregator import RollingAverageAggregator
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME_PREFIX = lambda n: f'aggregate_trip_distance_{n}'
FILTER_BY_DISTANCE_ROUTING_KEY = 'filter_by_distance'
LOG_FREQ = 100


class AggregateTripDistance(RollingAverageAggregator):
    def __init__(self,
                 rabbit_hostname: str,
                 aggregate_keys: Tuple[str, ...],
                 average_key: str,
                 aggregate_id: int,
                 producers: int = 1,
                 consumers: int = 1):
        super().__init__(rabbit_hostname, aggregate_keys, average_key)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME_PREFIX(aggregate_id),
            producers=producers
        )
        self._consumers = consumers
        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def run(self):
        self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message, delivery_tag):
        payload = message['payload']
        if isinstance(payload, list):
            for obj in payload:
                self.aggregate(payload=obj)

    def on_producer_finished(self, message, delivery_tag):
        for k, v in self._aggregate_table.items():
            message = {'type': 'distance_metric', 'payload': {'station': k, 'distance': v.current_average}}
            self.publish(json.dumps(message), self._output_exchange,
                         routing_key=FILTER_BY_DISTANCE_ROUTING_KEY)
        for _ in range(self._consumers):
            self.publish(json.dumps({'payload': 'EOF'}), self._output_exchange,
                         routing_key=FILTER_BY_DISTANCE_ROUTING_KEY)
        self.close()
