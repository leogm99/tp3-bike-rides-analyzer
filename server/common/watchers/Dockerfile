FROM server:latest

RUN pip install docker

COPY ./server/common/utils.py /common/utils.py
COPY ./common_utils /common_utils
COPY ./server/main.py /main.py
COPY ./server/common/middleware /common/middleware
COPY ./server/common/dag_node.py /common/dag_node.py
COPY ./server/common/dag_node_factory.py /common/dag_node_factory.py
COPY ./server/common/watched /common/watched
COPY ./server/common/watchers /common/watchers