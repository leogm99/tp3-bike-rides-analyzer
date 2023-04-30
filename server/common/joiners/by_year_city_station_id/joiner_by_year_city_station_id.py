import json
import logging
from typing import Tuple

from common.joiners.joiner import Joiner
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

STATIONS_EXCHANGE = 'join_by_year_city_station_id_stations'
STATIONS_EXCHANGE_TYPE = 'fanout'
QUEUE_NAME = 'join_by_year_city_station_id'
STATIC_DATA_ACK_ROUTING_KEY = 'static_data_ack'
AGGREGATE_TRIP_COUNT_ROUTING_KEY = 'aggregate_trip_count'


class JoinByYearCityStationId(Joiner):
    def __init__(self,
                 index_key: Tuple[str, ...],
                 rabbit_hostname: str,
                 stations_producers: int = 1,
                 trips_producers: int = 1):
        super().__init__(index_key, rabbit_hostname)
        self._stations_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name='',
            bind_exchange=STATIONS_EXCHANGE,
            bind_exchange_type=STATIONS_EXCHANGE_TYPE,
            producers=stations_producers,
        )
        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name='',
            bind_exchange='filter_by_year_output',
            bind_exchange_type='direct',
            producers=trips_producers,
        )
        self._output_exchange = RabbitExchange(
            self._rabbit_connection
        )

    def run(self):
        self._stations_input_queue.consume(self.on_message_callback, self.on_producer_finished, auto_ack=False)
        self._trips_input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message_obj, delivery_tag):
        payload = message_obj['payload']
        if message_obj['type'] == 'stations':
            if message_obj['payload'] != 'EOF':
                self.insert_into_side_table(payload)
            self._stations_input_queue.ack(delivery_tag=delivery_tag)
            return
        if isinstance(payload, list):
            join_data = []
            for obj in payload:
                obj['code'] = obj.pop('start_station_code')
                data = self.join(obj)
                if data is not None:
                    del data['city']
                    del data['code']
                    join_data.append(data)
            if len(join_data) == 0:
                join_data = None
        else:
            payload['code'] = payload.pop('start_station_code')
            join_data = self.join(payload)
            if join_data:
                del join_data['city']
                del join_data['code']
        if join_data is not None:
            hashes = self.hash_message(message=join_data, hashing_key='name', hash_modulo=1)
            if isinstance(hashes, dict):
                for _, obj in hashes.items():
                    self._output_exchange.publish(json.dumps({'payload': obj}),
                                                  routing_key=AGGREGATE_TRIP_COUNT_ROUTING_KEY)
            elif isinstance(join_data, dict):
                self._output_exchange.publish(json.dumps({'payload': join_data}),
                                              routing_key=AGGREGATE_TRIP_COUNT_ROUTING_KEY)

    def on_producer_finished(self, message, delivery_tag):
        if message['type'] == 'stations':
            self._stations_input_queue.ack(delivery_tag=delivery_tag)
            self._stations_input_queue.cancel()
            self._output_exchange.publish(json.dumps({'type': 'notify', 'payload': 'ack'}),
                                          routing_key=STATIC_DATA_ACK_ROUTING_KEY)
            logging.info(f'action: on-producer-finished | len-keys: {len(self._side_table.keys())}')
