import json
from typing import Tuple

from common.aggregators.aggregate_trip_distance.aggregate_trip_distance_middleware import \
    AggregateTripDistanceMiddleware
from common.aggregators.rolling_average_aggregator.rolling_average_aggregator import RollingAverageAggregator


class AggregateTripDistance(RollingAverageAggregator):
    def __init__(self,
                 aggregate_keys: Tuple[str, ...],
                 average_key: str,
                 consumers: int = 1,
                 middleware: AggregateTripDistanceMiddleware = None):
        super().__init__(aggregate_keys, average_key)
        self._middleware = middleware
        self._consumers = consumers

    def run(self):
        try:
            self._middleware.receive_trips_distances(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e

    def on_message_callback(self, message, delivery_tag):
        payload = message['payload']
        if isinstance(payload, list):
            for obj in payload:
                self.aggregate(payload=obj)

    def on_producer_finished(self, message, delivery_tag):
        for k, v in self._aggregate_table.items():
            message = {'type': 'distance_metric', 'payload': {'station': k, 'distance': v.current_average}}
            self._middleware.send_filter_message(json.dumps(message))
        for _ in range(self._consumers):
            self._middleware.send_filter_message(json.dumps({'payload': 'EOF'}))
        self._middleware.stop()

    def close(self):
        if not self.closed:
            super(AggregateTripDistance, self).close()
            self._middleware.stop()


