from common.aggregators.rolling_average_aggregator.rolling_average_aggregator import RollingAverageAggregator
from typing import Tuple

# para el rolling average, tengo que tener la cantidad total y un counter
from common.rabbit.rabbit_queue import RabbitQueue

ROUTING_KEY_PREFIX = 'aggregate_trip_duration'


class AggregateTripDuration(RollingAverageAggregator):
    def __init__(self, rabbit_hostname: str,
                 aggregate_keys: Tuple[str, ...],
                 average_key: str,
                 aggregate_id: int = 0):
        super().__init__(rabbit_hostname, aggregate_keys, average_key)
        self._routing_key = ROUTING_KEY_PREFIX + f'_{aggregate_id}'
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=self._routing_key,
        )

    def aggregate(self, message):
        payload = message['payload']
        if isinstance(payload, list):
            for obj in payload:
                obj['duration_sec'] = max(float(obj['duration_sec']), 0.)
                super(AggregateTripDuration, self).aggregate(obj)

    def run(self):
        self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message, delivery_tag):
        self.aggregate(message)

    def on_producer_finished(self, message, delivery_tag):
        pass
