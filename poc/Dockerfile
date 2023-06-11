FROM server:latest

# docker sdk for python
RUN pip install docker

COPY bully.py .
COPY restart.py .

CMD python3 bully.py