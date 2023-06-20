import logging

import docker


def restart_container(container_ip_addr):
    client = docker.from_env()
    containers = list(filter(lambda c: container_ip_addr in c[1],
                             map(lambda c: (c, c.attrs['Name']), client.containers.list(all=True))))
    if containers:
        logging.info(f'found container: {containers[0][0]}')
        containers[0][0].restart()
