FROM server:latest

# docker sdk for python
RUN pip install docker

COPY watcher.py .
COPY watched.py .
COPY ping_middleware.py .
COPY watcher_middleware.py .
COPY bully.py .
COPY leader_election.py .
COPY leader_election_middleware.py .
COPY singleton.py .
COPY restart.py .

CMD python3 bully.py