FROM python:3.10-slim-bullseye

RUN pip install pika
RUN pip install pyzmq