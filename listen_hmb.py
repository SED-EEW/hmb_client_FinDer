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
# from myprocessing import launch_funder as process_message
from my_processing import process_message


def shellprocess_manager_multithread(queue, maxprocess=3):
    running_processes = []
    while True:

        check_running_processes = [p for p in running_processes if p.poll() is None]

        if len(check_running_processes) > maxprocess:
            logging.debug('- Queue full, loop : %s', running_processes)
            time.sleep(1)
            continue

        msg = queue.get()
        try:
            p = process_message(msg)

            logging.debug('- Launch shell process : %s', p)
            check_running_processes.append(p)
        except Exception as e:
            logging.exception('Unexpected exception : %s', str(e))

        running_processes = check_running_processes


def shellprocess_manager_singlethread(queue):
    while True:

        msg = queue.get()
        tick = time.time()
        try:
            p = process_message(msg)
            p.wait()
            logging.info('End process in %.1f s with return code %d', time.time()-tick, p.returncode)
        except Exception as e:
            logging.exception('Unexpected exception : %s', str(e))


def launch_hmb(pqueue, hmbsession):
    def _process_closure(msg):
        logging.info('- hmb msg: %s', msg.keys())
        process_queue.put(msg)

    hmbsession.listen(_process_closure)


if __name__ == '__main__':
    argd = ArgumentParser()
    argd.add_argument('url', help='adresse of the hmb bserver')
    argd.add_argument('--cfg', help='config file for connexion parameters (e.g. queue, user, password)')
    argd.add_argument('--timeout', help='define timeout', type=int, default=60)
    argd.add_argument('--nlast', help='n last message to get backNumber of messages to backfill from the server', type=int, default=10)
    argd.add_argument('--queue', help='define the queue to listen')
    argd.add_argument('--user', help='connexion authentication')
    argd.add_argument('--password', help='connexion authentication')
    argd.add_argument('--singlethread', help='force single thread running (useful for debugging)', action='store_true')
    argd.add_argument('-v', '--verbose', action='store_true')

    args = argd.parse_args()
    dargs = vars(args)

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG if args.verbose else logging.INFO)

    if args.cfg is not None:
        cfg = load_hmbcfg(args.cfg)
    else:
        cfg = {}

    for k in ['queue', 'user', 'password']:
        if dargs[k] is not None:
            cfg[k] = dargs[k]

    logging.info('Configs : %s', cfg)

    url = args.url
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

    if args.queue is not None:
        queue = args.queue.split(',')
    else:
        queue = []

    hmb.queue(*queue, nlast=args.nlast)

    process_queue = Queue()

    hmbthread = Process(name='hmbthread', target=launch_hmb, args=(process_queue, hmb))
    hmbthread.start()

    if args.singlethread:
        shellprocess_manager_singlethread(process_queue)
    else:
        shellprocess_manager_multithread(process_queue)

    hmbthread.join()
