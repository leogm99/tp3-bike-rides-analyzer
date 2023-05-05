import logging

import pika
from typing import Union


class RabbitBlockingConnection:
    def __init__(self, rabbit_hostname):
        self._conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbit_hostname,
                heartbeat=1000  # Higher than default heartbeat to avoid connection shutdown
            )
        )
        self._channel = self._conn.channel()
        self._channel.basic_qos(prefetch_count=50)
        self._closed = False
        self._consuming = False

    def queue_declare(self, queue_name: str) -> str:
        result = self._channel.queue_declare(
            queue=queue_name,
            durable=True,
        )
        return result.method.queue

    def queue_bind(self, queue_name: str, exchange_name: str, routing_key: str):
        self._channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key=routing_key,
        )

    def exchange_declare(self, exchange_name: str, exchange_type: str):
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
        )

    def publish(self, message: Union[str, bytes], exchange_name: str, routing_key: str):
        if isinstance(message, str):
            message = message.encode('utf8')
        self._channel.basic_publish(
            body=message,
            exchange=exchange_name,
            routing_key=routing_key,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def consume(self, queue_name: str, on_message_callback, auto_ack=True):
        return self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=on_message_callback,
            auto_ack=auto_ack,
        )

    def start_consuming(self):
        self._consuming = True
        self._channel.start_consuming()

    def cancel_consumer(self, consumer_tag):
        self._channel.basic_cancel(consumer_tag)

    def ack(self, delivery_tag):
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def close(self):
        if not self._closed:
            self._closed = True
            if self._consuming:
                self._channel.stop_consuming()
            self._conn.close()
