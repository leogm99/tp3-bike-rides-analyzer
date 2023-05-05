import json
import logging

from common.aggregators.count_aggregator.count_aggregator import CountAggregator
from typing import Tuple

from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME_PREFIX = lambda n: f'aggregate_trip_count_{n}'
FILTER_BY_COUNT_ROUTING_KEY = 'filter_by_count'


class AggregateTripCount(CountAggregator):
    def __init__(self,
                 rabbit_hostname: str,
                 aggregate_keys: Tuple[str, ...],
                 aggregate_id: int = 0,
                 producers: int = 1,
                 consumers: int = 1):
        super().__init__(rabbit_hostname, aggregate_keys)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME_PREFIX(aggregate_id),
            producers=producers
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

        self._consumers = consumers

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
                try:
                    year = int(obj['yearid'])
                except ValueError as e:
                    logging.error(f'action: on-message-callback | data-error: {e}')
                    continue
                if year == 2016:
                    self.aggregate(payload=obj, year_2016=1, year_2017=0)
                elif year == 2017:
                    self.aggregate(payload=obj, year_2016=0, year_2017=1)

    def on_producer_finished(self, message, delivery_tag):
        for k, v in self._aggregate_table.items():
            message = {'payload': {'station': k, 'year_2016': v['year_2016'], 'year_2017': v['year_2017']}}
            self.publish(json.dumps(message), self._output_exchange, routing_key=FILTER_BY_COUNT_ROUTING_KEY)
        for _ in range(self._consumers):
            self.publish(json.dumps({'payload': 'EOF'}), self._output_exchange, routing_key=FILTER_BY_COUNT_ROUTING_KEY)
        self.close()
