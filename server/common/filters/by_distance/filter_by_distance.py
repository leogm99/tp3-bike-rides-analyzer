from common.filters.numeric_range.numeric_range import NumericRange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME = 'filter_by_distance'


class FilterByDistance(NumericRange):
    def __init__(self, filter_key: str,
                 low: float,
                 high: float,
                 rabbit_hostname: str,
                 keep_filter_key: bool = False,
                 producers: int = 1):
        super().__init__(filter_key, low, high, rabbit_hostname, keep_filter_key)
        self._input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=producers,
        )

    def run(self):
        self._input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message, _delivery_tag):
        to_send, message_obj = super(FilterByDistance, self).on_message_callback(message, _delivery_tag)

    def on_producer_finished(self, message, delivery_tag):
        pass
