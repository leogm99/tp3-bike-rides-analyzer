import json
import logging
from typing import Tuple

from common.joiners.joiner import Joiner
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

QUEUE_NAME_TRIPS = 'joiner_by_year_end_station_id'

FILTER_BY_CITY_STATIONS_EXCHANGE = 'filter_by_city_stations_output'
FILTER_BY_CITY_STATIONS_EXCHANGE_TYPE = 'fanout'

STATIC_DATA_ACK_ROUTING_KEY = 'static_data_ack'
HAVERSINE_APPLIER_ROUTING_KEY = 'haversine_applier'


class JoinByYearEndStationId(Joiner):
    def __init__(self,
                 index_key: Tuple[str, ...],
                 rabbit_hostname: str,
                 stations_producers: int = 1,
                 trips_producers: int = 1,
                 consumers: int = 1):
        super().__init__(index_key, rabbit_hostname)
        self._stations_input_queue = RabbitQueue(
            self._rabbit_connection,
            bind_exchange=FILTER_BY_CITY_STATIONS_EXCHANGE,
            bind_exchange_type=FILTER_BY_CITY_STATIONS_EXCHANGE_TYPE,
            producers=stations_producers,
        )

        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME_TRIPS,
            producers=trips_producers,
        )

        self._output_exchange = RabbitExchange(
            self._rabbit_connection,
        )

        self._consumers = consumers

    def run(self):
        self._stations_input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._trips_input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message, delivery_tag):
        if message['type'] == 'stations':
            if message['payload'] != 'EOF':
                self.insert_into_side_table(message['payload'])
            return
        payload = message['payload']
        if payload == 'EOF':
            return
        buffer = []
        for obj in payload:
            start_station_code = obj.pop('start_station_code')
            end_station_code = obj.pop('end_station_code')
            obj['code'] = start_station_code
            start_station_join = self.join(obj)
            obj['code'] = end_station_code
            end_station_join = self.join(obj)
            if start_station_join and end_station_join:
                start_station_join['start_station_latitude'] = start_station_join.pop('latitude')
                start_station_join['start_station_longitude'] = start_station_join.pop('longitude')
                end_station_join['end_station_latitude'] = end_station_join.pop('latitude')
                end_station_join['end_station_longitude'] = end_station_join.pop('longitude')
                end_station_join['end_station_name'] = end_station_join.pop('name')
                del start_station_join['name']
                del start_station_join['code']
                del start_station_join['yearid']
                del end_station_join['yearid']
                del end_station_join['code']
                result = start_station_join | end_station_join
                buffer.append(result)
        if buffer:
            self.__send_message(json.dumps({'payload': buffer}))

    def __send_message(self, message):
        self.publish(message, self._output_exchange, routing_key=HAVERSINE_APPLIER_ROUTING_KEY)

    def on_producer_finished(self, message, delivery_tag):
        if message['type'] == 'stations':
            self._stations_input_queue.cancel()
            self.publish(json.dumps({'type': 'notify', 'payload': 'ack'}), self._output_exchange,
                         routing_key=STATIC_DATA_ACK_ROUTING_KEY)
            logging.info(f'action: on-producer-finished | len-keys: {len(self._side_table.keys())}')
        elif message['type'] == 'trips':
            logging.info(f'action: on-producer-finished | received END OF STREAM: {message}')
            for _ in range(self._consumers):
                self.publish(json.dumps({'payload': 'EOF'}), self._output_exchange,
                             routing_key=HAVERSINE_APPLIER_ROUTING_KEY)
            self.close()
