#!/usr/bin/env python3

import sys
import time

import getpass
import logging
from argparse import ArgumentParser
from multiprocessing import Queue, Process

from emschmb import EmscHmbListener, load_hmbcfg


# here you can import the function you want to launch
# BUT it has to be named 'process_message'
# one example complete
from my_processing import process_message
# from feltreport_processing import process_message

__version__ = '1.01'


def _process_wrapper(p, msg, tag):
    try:
        tick = time.time()
        p(msg)
        logging.info('%s ended in %.1f s', tag, time.time()-tick)
    except Exception as e:
        logging.exception("Unexpected exception during message processing: %s", str(e))


def shellprocess_manager_multithread(hmb, maxprocess=3):
    process_queue = Queue()
    hmbthread = Process(name='hmbthread', target=launch_hmb, args=(process_queue, hmb))
    hmbthread.start()

    local_pid = 1
    running_processes = []
    while True:

        check_running_processes = [p for p in running_processes if p.is_alive() is True]

        if len(check_running_processes) >= maxprocess:
            logging.debug('- Queue full, loop : %s', running_processes)
            time.sleep(1)
            continue

        msg = process_queue.get()
        try:
            tag = 'Process_{0}'.format(local_pid)
            p = Process(name=tag, target=_process_wrapper, args=(process_message, msg, tag))
            p.start()

            local_pid += 1

            logging.debug('- Launch shell process : %s -> %s', tag, p)
            check_running_processes.append(p)
        except Exception as e:
            logging.exception('Unexpected exception : %s', str(e))

        running_processes = check_running_processes
    hmbthread.join()


def shellprocess_manager_singlethread(hmb):
    process_queue = Queue()
    hmbthread = Process(name='hmbthread', target=launch_hmb, args=(process_queue, hmb))
    hmbthread.start()

    while True:

        msg = process_queue.get()
        tick = time.time()
        try:
            process_message(msg)
            logging.info('End process in %.1f s', time.time()-tick)
        except Exception as e:
            logging.exception('Unexpected exception : %s', str(e))

    hmbthread.join()


def shellprocess_manager_nothread(hmb):
    logging.debug('Begin hmb listener...')
    hmb.listen(process_message)


def launch_hmb(pqueue, hmbsession):
    def _process_closure(msg):
        logging.info('- hmb msg: %s', msg.keys())
        pqueue.put(msg)

    logging.debug('Begin hmb listener...')
    hmbsession.listen(_process_closure)


if __name__ == '__main__':
    argd = ArgumentParser()
    argd.add_argument('url', help='adresse of the hmb bserver')
    argd.add_argument('--cfg', help='config file for connexion parameters (e.g. queue, user, password)')
    argd.add_argument('--timeout', help='define timeout', type=int, default=30)
    argd.add_argument('--nlast', help='n last message to get backNumber of messages to backfill from the server', type=int, default=10)
    argd.add_argument('--queue', help='define the queue to listen')
    argd.add_argument('--user', help='connexion authentication')
    argd.add_argument('--password', help='connexion authentication')
    argd.add_argument('--nthreads', help='number of concurrent running threads', type=int, default=3)
    argd.add_argument('--singlethread', help='force single thread running (useful for debugging)', action='store_true')
    argd.add_argument('--nothread', help='force no threading (useful for debugging)', action='store_true')
    argd.add_argument('-v', '--verbose', action='store_true')

    args = argd.parse_args()
    dargs = vars(args)

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG if args.verbose else logging.INFO)

    logging.info('Listen HMB (%s)', __version__)

    if args.cfg is not None:
        cfg = load_hmbcfg(args.cfg)
    else:
        cfg = {}

    for k in ['queue', 'user', 'password']:
        if dargs[k] is not None:
            cfg[k] = dargs[k]

    logging.info('Configs : %s', cfg)

    url = args.url
    if 'queue' not in cfg:
        argd.error('queue parameter is mandatory in cmd or cfg')

    queue = cfg['queue']
    user = cfg.get('user')
    password = cfg.get('password')

    if user is not None and password is None:
        password = getpass.getpass('Password for {0} : '.format(user))

    heartbeat = args.timeout / 2
    hmb = EmscHmbListener(url, heartbeat=heartbeat)

    auth = None
    if user is not None and password is not None:
        logging.info('Use authentication')
        hmb.authentication(user, password)

    queue = queue.split(',')

    hmb.queue(*queue, nlast=args.nlast)

    if args.nothread:
        logging.info('No thread processing')
        shellprocess_manager_nothread(hmb)
    elif args.singlethread:
        logging.info('Single thread processing')
        shellprocess_manager_singlethread(hmb)
    else:
        logging.info('Multi threads processing (%d process(es))', args.nthreads)
        shellprocess_manager_multithread(hmb, maxprocess=args.nthreads)
