import json
import logging

from common.aggregators.aggregate_trip_duration.aggregate_trip_duration_middleware import \
    AggregateTripDurationMiddleware
from common.aggregators.rolling_average_aggregator.rolling_average_aggregator import RollingAverageAggregator
from typing import Tuple


class AggregateTripDuration(RollingAverageAggregator):
    def __init__(self, aggregate_keys: Tuple[str, ...],
                 average_key: str,
                 middleware: AggregateTripDurationMiddleware):
        super().__init__(aggregate_keys, average_key)
        self._middleware = middleware

    def run(self):
        try:
            self._middleware.receive_trip_duration(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, delivery_tag):
        payload = message['payload']
        if isinstance(payload, list):
            for obj in payload:
                obj['duration_sec'] = max(float(obj['duration_sec']), 0.)
                super(AggregateTripDuration, self).aggregate(payload=obj)

    def on_producer_finished(self, message, delivery_tag):
        logging.info('action: on-producer-finished | END OF STREAM RECEIVED')
        for k, v in self._aggregate_table.items():
            message = {'type': 'duration_metric', 'payload': {'date': k, 'duration_sec': v.current_average}}
            self._middleware.send_metrics_message(json.dumps(message))
        self._middleware.send_metrics_message(json.dumps({'type': 'duration_metric', 'payload': 'EOF'}))
        self._middleware.stop()
