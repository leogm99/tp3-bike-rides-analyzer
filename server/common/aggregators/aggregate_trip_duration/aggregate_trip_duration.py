import json
import logging

from common.aggregators.rolling_average_aggregator.rolling_average_aggregator import RollingAverageAggregator
from typing import Tuple

from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

ROUTING_KEY_PREFIX = 'aggregate_trip_duration'
METRICS_CONSUMER_ROUTING_KEY = 'metrics_consumer'


class AggregateTripDuration(RollingAverageAggregator):
    def __init__(self, rabbit_hostname: str,
                 aggregate_keys: Tuple[str, ...],
                 average_key: str,
                 aggregate_id: int = 0,
                 producers: int = 1):
        super().__init__(rabbit_hostname, aggregate_keys, average_key)
        self._routing_key = ROUTING_KEY_PREFIX + f'_{aggregate_id}'
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=self._routing_key,
            producers=producers
        )
        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

    def run(self):
        try:
            self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
            self._rabbit_connection.start_consuming()
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
            self.publish(json.dumps(message), exchange=self._output_exchange,
                         routing_key=METRICS_CONSUMER_ROUTING_KEY)
        self.publish(json.dumps({'type': 'duration_metric', 'payload': 'EOF'}),
                     self._output_exchange,
                     routing_key=METRICS_CONSUMER_ROUTING_KEY)
        self.close()
