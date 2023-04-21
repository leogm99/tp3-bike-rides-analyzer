import logging

import pika


class TripsConsumer:
    def __init__(self,
                 rabbit_hostname: str,
                 data_exchange: str,
                 exchange_type: str,
                 trips_queue_name: str):
        self.received = 0
        self._rabbit_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbit_hostname)
        )
        self._channel = self._rabbit_connection.channel()
        self._channel.exchange_declare(
            exchange=data_exchange,
            exchange_type=exchange_type,
            durable=True
        )
        self._channel.queue_declare(
            queue=trips_queue_name,
            durable=True,
        )
        self._channel.queue_bind(
            queue=trips_queue_name,
            exchange=data_exchange,
            routing_key=trips_queue_name,
        )
        self._queue_name = trips_queue_name

    def run(self):
        self._channel.basic_consume(queue=self._queue_name,
                                    on_message_callback=lambda ch, method, properties, body: self.callback(ch,
                                                                                                           method,
                                                                                                           properties,
                                                                                                           body),
                                    auto_ack=True,)
        self._channel.start_consuming()
        pass

    def callback(self, ch, method, properties, body):
        self.received += 1
