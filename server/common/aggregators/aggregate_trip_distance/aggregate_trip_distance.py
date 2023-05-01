from typing import Tuple

from common.aggregators.rolling_average_aggregator.rolling_average_aggregator import RollingAverageAggregator
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'aggregate_trip_distance'
LOG_FREQ = 100


class AggregateTripDistance(RollingAverageAggregator):
    def __init__(self,
                 rabbit_hostname: str,
                 aggregate_keys: Tuple[str, ...],
                 average_key: str):
        super().__init__(rabbit_hostname, aggregate_keys, average_key)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
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
        pass
