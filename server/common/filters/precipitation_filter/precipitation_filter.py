import json
import logging

from datetime import datetime, timedelta
from common.dag_node import DAGNode
from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange
from common.utils import select_message_fields, message_from_payload

DATE_FORMAT: str = '%Y-%m-%d'
CORRECTION_DELTA = timedelta(days=1)


class PrecipitationFilter(DAGNode):
    precipitation_threshold = 30
    mean_trip_time_joiner_fields = ['date']

    def __init__(self,
                 rabbit_hostname: str,
                 queue_name: str,
                 mean_trip_time_joiner_exchange: str,
                 mean_trip_time_joiner_exchange_type: str):
        self.count = 0
        super().__init__()
        self._rabbit_connection = RabbitBlockingConnection(
            rabbit_hostname=rabbit_hostname,
        )
        self._input_queue = RabbitQueue(
            rabbit_connection=self._rabbit_connection,
            queue_name=queue_name,
        )
        self._mean_trip_time_joiner_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
            exchange_name=mean_trip_time_joiner_exchange,
            exchange_type=mean_trip_time_joiner_exchange_type,
        )

    def run(self):
        self._input_queue.consume(self.on_message_callback)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message):
        message_obj = json.loads(message)
        payload = message_obj['payload']

        if not isinstance(payload, str):
            precipitation = float(payload['prectot'])
            if precipitation > self.precipitation_threshold:
                weather_date = datetime.strptime(payload['date'], DATE_FORMAT)
                corrected_weather_date = weather_date - CORRECTION_DELTA
                payload['date'] = corrected_weather_date.strftime(DATE_FORMAT)
                self.__send_to_mean_trip_time_joiner(payload)
                self.count += 1
        else:
            logging.info(f'sending eof after {self.count} messages')
            self.publish(message, self._mean_trip_time_joiner_exchange)

    @select_message_fields(fields=mean_trip_time_joiner_fields)
    @message_from_payload(message_type='weather')
    def __send_to_mean_trip_time_joiner(self, message):
        self.publish(message, self._mean_trip_time_joiner_exchange)

    def close(self):
        if not self.closed:
            self.closed = True
            self._rabbit_connection.close()
