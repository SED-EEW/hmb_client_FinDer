#!/usr/bin/env python3

import sys
import getpass
import logging
from argparse import ArgumentParser
import json

from emschmb import EmscHmbListener, load_hmbcfg, readstdin


# here you can import the function you want to launch
# BUT it has to be named 'process_message'
# for example
from my_processing import process_message

__version__ = '1.0'


def display(msg):
    msg.pop('data', None)
    print('message:', msg)


if __name__ == '__main__':
    argd = ArgumentParser()
    argd.add_argument('query', help='select messages with query (json format, mongodb syntax)', nargs='?')
    argd.add_argument('--check', help='only display results and skip process_message', action='store_true')
    argd.add_argument('--url', help='adresse of the hmb bserver')
    argd.add_argument('--cfg', help='config file for connexion parameters (e.g. queue, user, password)')
    argd.add_argument('--queue', help='define the queue to listen')
    argd.add_argument('--user', help='connexion authentication')
    argd.add_argument('--password', help='connexion authentication')
    argd.add_argument('-v', '--verbose', action='store_true')

    args = argd.parse_args()
    dargs = vars(args)

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG if args.verbose else logging.INFO)
    logging.info('Replay HMB message (%s)', __version__)

    filter = json.loads(args.query or ''.join(readstdin()))
    logging.info('Filtering query: %s', filter)

    if args.cfg is not None:
        cfg = load_hmbcfg(args.cfg)
    else:
        cfg = {}

    for k in ['url', 'queue', 'user', 'password']:
        if dargs[k] is not None:
            cfg[k] = dargs[k]

    cfgnopassword = cfg.copy()
    if 'password' in cfg:
        cfgnopassword['password'] = '****'

    logging.info('Configs : %s', cfgnopassword)

    if 'queue' not in cfg:
        argd.error('queue parameter is mandatory in cmd or cfg')
    elif 'url' not in cfg:
        argd.error('url parameter is mandatory in cmd or cfg')

    url = cfg['url']
    queue = cfg['queue']
    user = cfg.get('user')
    password = cfg.get('password')

    if user is not None and password is None:
        password = getpass.getpass('Password for {0} : '.format(user))

    hmb = EmscHmbListener(url, heartbeat=15)

    auth = None
    if user is not None and password is not None:
        logging.info('Use authentication')
        hmb.authentication(user, password)

    hmb.get(display if args.check else process_message, queue, filter)
