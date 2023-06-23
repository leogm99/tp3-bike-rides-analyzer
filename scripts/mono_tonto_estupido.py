#!/usr/bin/env python3

"""
    Mono Tonto Estúpido 🐵🐒

    This dumb script brings down nodes in the Bike Rides Analyzer system.

    Its only purpose is to test if the system actually handles failures.

    If not specified in the arguments, it will try to kill periodically one or a group of nodes.
"""

import argparse
import signal
import logging
import random
import threading
import subprocess

# MAYBE_TODO: dehardcode
NETWORK = 'tp3-bike-rides-analyzer_default'
STOP = threading.Event()
MIN_KILL_RATE_SECONDS = 10
MAX_KILL_RATE_SECONDS = 60

def sig_handler():
    logging.info('action: sig-handler | message: quitting now')
    STOP.set()

def main():
    signal.signal(signalnum=signal.SIGTERM, handler=lambda *_: sig_handler())
    signal.signal(signalnum=signal.SIGINT, handler=lambda *_: sig_handler())
    if args.periodically:
        kill_periodically()
    else:
        kill_once()

def get_kill_rate():
    return args.kill_rate if args.kill_rate is not None else random.randint(a=MIN_KILL_RATE_SECONDS, b=MAX_KILL_RATE_SECONDS)

def get_hosts():
    result = subprocess.run(['docker', 'network', 'inspect', NETWORK, '--format="{{range $key, $value := .Containers}}{{println $value.Name}}{{end}}"'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    all_host_list = result.stdout.decode().replace('"', '').split('\n')
    # TODO: remove loader from here
    all_host_list = list(filter(lambda h: all(ignore_host not in h for ignore_host in ['loader', 'client', 'rabbit']) and h != '', all_host_list))
    hosts = []
    if args.host_kill_list:
        for user_host in args.host_kill_list:
            for host in all_host_list:
                if user_host in host:
                    hosts.append(host)
    else: 
        hosts = all_host_list
    return hosts

def get_hosts_to_kill():
    hosts = get_hosts()
    to_kill = choose_random(hosts) if args.at_random else hosts
    leave_one_watcher_alive(hosts, to_kill)
    return to_kill

def choose_random(_list):
    n = random.randint(0, len(_list))
    return random.sample(_list, k=n)

def leave_one_watcher_alive(all_hosts: list, hosts_to_kill: list):
    is_watcher = lambda h: 'watcher' in h
    n_watchers = len(list(filter(is_watcher, all_hosts)))
    watchers_to_kill = list(filter(is_watcher, hosts_to_kill))
    if n_watchers != 0 and len(watchers_to_kill) == n_watchers:
        spare_random_watcher = watchers_to_kill[random.randint(0, len(watchers_to_kill) - 1)]
        spare_idx = hosts_to_kill.index(spare_random_watcher)
        del hosts_to_kill[spare_idx]

def kill_once():
    to_kill = get_hosts_to_kill()
    kill_containers(to_kill)

def kill_periodically():
    while not STOP.wait(timeout=get_kill_rate()):
        kill_once()

def kill_containers(to_kill):
    for c in to_kill:
        subprocess.run(['docker', 'kill', c])
    logging.info(f'killed all these hosts: {to_kill}')


if __name__ == '__main__':
    logging.basicConfig(level='INFO')

    parser = argparse.ArgumentParser()

    parser.add_argument('--host_kill_list', help='choose at random from a list of hosts', type=str, nargs='+')
    parser.add_argument('--at_random', help='(boolean) select nodes at random from the input', action='store_true')
    parser.add_argument('--not_at_random', help='(boolean) do not select nodes at random from the input', dest='at_random', action='store_false')
    parser.add_argument('--periodically', help='(boolean) periodically try to kill nodes', action='store_true')
    parser.add_argument('--not_periodically', help='(boolean) kills once and terminates', dest='periodically', action='store_false')
    parser.add_argument('--kill_rate', help='if periodically is set, the kill rate in seconds can be specified.'
                                            'if not specified, nodes will be killed at random intervals', type=int)
    parser.set_defaults(periodically=True)
    parser.set_defaults(at_random=True)

    args = parser.parse_args()

    main()
