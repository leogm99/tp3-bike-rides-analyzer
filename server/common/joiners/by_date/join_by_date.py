import json
import logging
from typing import Tuple, Dict
from datetime import datetime, timedelta

from common.joiners.joiner import Joiner
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

WEATHER_EXCHANGE_NAME = 'join_by_date_weather'
WEATHER_EXCHANGE_TYPE = 'fanout'
QUEUE_NAME = 'join_by_date'
STATIC_DATA_ACK_ROUTING_KEY = 'static_data_ack'
AGGREGATE_TRIP_DURATION_ROUTING_KEY = lambda n: f'aggregate_trip_duration_{n}'
DELTA_CORRECTION = timedelta(days=1)


class JoinByDate(Joiner):
    def __init__(self,
                 index_key: Tuple[str, ...],
                 rabbit_hostname: str,
                 weather_producers: int = 1,
                 trips_producers: int = 1,
                 consumers: int = 1):
        super().__init__(index_key, rabbit_hostname)

        self._weather_date_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name='',
            bind_exchange=WEATHER_EXCHANGE_NAME,
            bind_exchange_type=WEATHER_EXCHANGE_TYPE,
            producers=weather_producers,
        )

        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=trips_producers,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )
        self._consumers = consumers
        self._msg_count = 0

    def run(self):
        self._weather_date_input_queue.consume(self.on_message_callback,
                                               self.on_producer_finished,
                                               auto_ack=False)
        self._trips_input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_eof_threshold_reached(self, message_type: str):
        pass

    def on_message_callback(self, message, delivery_tag):
        payload = message['payload']
        if message['type'] == 'weather':
            if payload == 'EOF':
                self._weather_date_input_queue.ack(delivery_tag=delivery_tag)
                return
            if isinstance(payload, list):
                for obj in payload:
                    weather_date = datetime.strptime(obj['date'], "%Y-%m-%d") - DELTA_CORRECTION
                    obj['date'] = weather_date.strftime('%Y-%m-%d')
            else:
                weather_date = datetime.strptime(payload['date'], "%Y-%m-%d") - DELTA_CORRECTION
                payload['date'] = weather_date.strftime('%Y-%m-%d')
            self.insert_into_side_table(payload, save_key='date')
            self._weather_date_input_queue.ack(delivery_tag=delivery_tag)
            return
        try:
            self._msg_count += 1
            if isinstance(payload, list):
                join_data = []
                for obj in payload:
                    obj['date'] = obj.pop('start_date')
                    trip_date = datetime.strptime(obj['date'], '%Y-%m-%d %H:%M:%S')
                    obj['date'] = datetime.strftime(trip_date, '%Y-%m-%d')
                    data = self.join(obj)
                    if data is not None:
                        del data['city']
                        join_data.append(data)
                if len(join_data) == 0:
                    join_data = None
            else:
                payload['date'] = payload.pop('start_date')
                trip_date = datetime.strptime(payload['date'], '%Y-%m-%d %H:%M:%S')
                payload['date'] = datetime.strftime(trip_date, '%Y-%m-%d')
                join_data = self.join(payload)
                if join_data is not None:
                    del join_data['city']
            if join_data:
                hashes = self.hash_message(message=join_data, hashing_key='date', hash_modulo=self._consumers)
                if isinstance(hashes, dict):
                    for routing_key_postfix, message_buffer in hashes.items():
                        self._output_exchange.publish(json.dumps({'payload': message_buffer}),
                                                      routing_key=AGGREGATE_TRIP_DURATION_ROUTING_KEY(
                                                          routing_key_postfix))
                elif isinstance(join_data, dict):
                    '''
                    self.hash_message()
                    consumer = self.hash_output(join_data['date'])
                    self._output_exchange.publish(json.dumps({'payload': join_data}),
                                                  routing_key=AGGREGATE_TRIP_DURATION_ROUTING_KEY(consumer))
                    '''
        except BaseException as e:
            logging.info(f'error: {e}')

    def on_producer_finished(self, message, delivery_tag):
        if message['type'] == 'weather':
            self._weather_date_input_queue.ack(delivery_tag=delivery_tag)
            self._weather_date_input_queue.cancel()
            self._output_exchange.publish(json.dumps({'type': 'notify', 'payload': 'ack'}),
                                          routing_key=STATIC_DATA_ACK_ROUTING_KEY)
            logging.info(f'action: on-producer-finished | len-keys: {len(self._side_table.keys())}')
