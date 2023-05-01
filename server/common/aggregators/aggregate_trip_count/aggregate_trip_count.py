import logging

from common.aggregators.count_aggregator.count_aggregator import CountAggregator
from typing import Tuple

from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'aggregate_trip_count'
FILTER_BY_COUNT_ROUTING_KEY = 'filter_by_count'


class AggregateTripCount(CountAggregator):
    def __init__(self, rabbit_hostname: str, aggregate_keys: Tuple[str, ...]):
        super().__init__(rabbit_hostname, aggregate_keys)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
        )

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
        pass
