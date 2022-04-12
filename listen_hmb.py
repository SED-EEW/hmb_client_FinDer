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


def _process_wrapper(p, msg, tag):
    try:
        tick = time.time()
        p(msg)
        logging.info('%s ended in %.1f s', tag, time.time()-tick)
    except Exception as e:
        logging.exception("Unexpected exception during message processing: %s", str(e))


def shellprocess_manager_multithread(queue, maxprocess=3):
    local_pid = 1
    running_processes = []
    while True:

        check_running_processes = [p for p in running_processes if p.is_alive() is True]

        if len(check_running_processes) >= maxprocess:
            logging.debug('- Queue full, loop : %s', running_processes)
            time.sleep(1)
            continue

        msg = queue.get()
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


def shellprocess_manager_singlethread(queue):
    while True:

        msg = queue.get()
        tick = time.time()
        try:
            process_message(msg)
            logging.info('End process in %.1f s', time.time()-tick)
        except Exception as e:
            logging.exception('Unexpected exception : %s', str(e))


def launch_hmb(pqueue, hmbsession):
    def _process_closure(msg):
        logging.info('- hmb msg: %s', msg.keys())
        pqueue.put(msg)

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
    argd.add_argument('--nthreads', help='number of concurrent running threads', type=int, default=3)
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

    hmb.queue(*queue, nlast=args.nlast)

    process_queue = Queue()

    hmbthread = Process(name='hmbthread', target=launch_hmb, args=(process_queue, hmb))
    hmbthread.start()

    if args.singlethread:
        shellprocess_manager_singlethread(process_queue)
    else:
        shellprocess_manager_multithread(process_queue, maxprocess=args.nthreads)

    hmbthread.join()
