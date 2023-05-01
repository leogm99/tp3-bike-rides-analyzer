import logging

from common.filters.numeric_range.numeric_range import NumericRange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'filter_by_count'


class FilterByCount(NumericRange):
    def __init__(self, filter_key: str, low: float, high: float, rabbit_hostname: str, keep_filter_key: bool = False):
        super().__init__(filter_key, low, high, rabbit_hostname, keep_filter_key)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
        )

    def run(self):
        self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message_obj, delivery_tag):
        payload = message_obj['payload']
        if isinstance(payload, list):
            buffer = []
            for obj in payload:
                super().high = 2 * float(obj['year_2017'])
                to_send, obj = super(FilterByCount, self).on_message_callback(obj, delivery_tag)
                if to_send:
                    buffer.append(obj)
            logging.info(buffer)
            # self._output_exchange.publish(json.dumps({'payload': buffer}))

    def on_producer_finished(self, message, delivery_tag):
        pass
