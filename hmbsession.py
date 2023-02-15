
"""
Module for sending messages to an httpmsgbus server.
"""
from __future__ import print_function
import sys
import requests
import time
import json
import logging
import bson
import datetime
import getpass

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _check_requests_status_raise(r):
    if r.status_code == 400:
        raise requests.exceptions.RequestException("bad request: " + r.text.strip())
    elif r.status_code == 503:
        raise requests.exceptions.RequestException("service unavailable: " + r.text.strip())
    r.raise_for_status()


def generic_hmb_display(msg):
    print(" * {0} --> New message".format(datetime.datetime.now()))
    for k, v in msg.items():
        if isinstance(v, dict):
            print('{0:10} : {1}'.format(k, '{}'))
            for kk, vv in v.items():
                print('{0:>20} : {1}'.format(kk, str(vv)[:70]))
        elif isinstance(v, list):
            print('{0:10} : {1}'.format(k, '[]'))
            for vv in v:
                print(' ' * 10, str(vv)[:70])
        else:
            print('{0:10} : {1}'.format(k, str(v)[:70]))
    print()


class HmbSession(object):
    def __init__(self, url, param=None, retry_wait=1, use_bson=False,
                 autocreate_queues=False):
        """opens a session with an hmb server at provided url.

       param = {
                "cid": <string>,
                "heartbeat": <int>,
                "recv_limit": <int>,
                "queue": {
                    <queue_name>: {
                        "topics": <list of string>,
                        "seq": <int>,
                        "endseq": <int>,
                        "starttime": <string>,
                        "endtime": <string>,
                        "filter": <doc>,
                        "qlen": <int>,
                        "oowait": <int>,
                        "keep": <bool>
                        },
                    ...
                    },
                }
       """
        self.url = url
        self.param = param or {}
        self.auth = None
        self.requests_kwargs = {}
        self._logger = logging.getLogger(__name__)
        self.retry_wait = retry_wait

        self._http_persistant = None

        self._close()
        self._oid = ''
        # flag selecting format of messages: either json or bson
        self._use_json = not use_bson
        # This might sometimes prevent issues over missing queues
        self._autocreate_queues = autocreate_queues

        """
        if 'queue' in self.param:
            # initially set 'keep' to false to get pending data
            for q in self.param['queue'].values():
                q['keep'] = False
        """

    def authentication(self, user, password):
        """Add authentication information

        Args:
            user (str): login username
            password (str): login password

        Returns:
            oject itself
        """
        self.auth = (user, password)
        return self

    def requests_args(self, **kwargs):
        """possibility to add parameters for requests module

        Returns:
            oject itself
        """
        self.requests_kwargs = kwargs
        return self

    def use_bson(self):
        """defines whether connection object should use bson or json
        note that this may force the connection to the server to be
        reestablished.
        """
        if self._use_json:
            self._use_json = False
            self._close()
        return self

    def use_json(self):
        if not self._use_json:
            self._use_json = True
            self._close()
        return self

    def get_httpsession(self):
        if self._http_persistant is None:
            self._logger.debug('New http session')
            self._http_persistant = requests.Session()
            self._http_persistant.auth = self.auth
        return self._http_persistant

    def close(self):
        if self._http_persistant is not None:
            self._http_persistant.close()

    def _open(self):
        """opens the HMB session"""
        try:
            headers = {"Content-type": "application/json" if self._use_json else "application/bson"}
            url = self.url + '/open'
            r = self.get_httpsession().post(
                url,
                data=json.dumps(self.param) if self._use_json else bson.BSON.encode(self.param),
                headers=headers, **self.requests_kwargs)
            self._logger.debug('Open %s with status %s', url, r.status_code)
            _check_requests_status_raise(r)

            ack = r.json() if self._use_json else bson.BSON(r.content).decode()
            self._sid = ack['sid']
            self._oid = ''
            self.param['cid'] = ack['cid']

            qinfo = ack.get('queue', {})
            for qname, queue in qinfo.items():
                error = queue.get('error', None)
                if error is not None:
                    self._logger.warning(
                        "HMB server error for queue '%s': %s",
                        qname, error)

                    # if queue not found then create queue?
                    if error == u'queue not found' and self._autocreate_queues:
                        msg = {'type': 'TOUCH', 'queue': qname}
                        self.send({'0': msg} if self._use_json else msg)
                        self.param['queue'][qname]['seq'] = 1
                        self._logger.info("Create HMB queue '%s' with TOUCH", qname)
                else:
                    # suppose that seq is alway a number!
                    seqnext = int(queue['seq'])
                    if qname in self.param['queue']:
                        if seqnext > self.param['queue'][qname].get('seq', 0):
                            self.param['queue'][qname]['seq'] = seqnext

            self._logger.info("New HMB session : url=%s, sid=%s, cid=%s",
                              self.url, ack['sid'], ack['cid'])
            self._logger.debug("Session parameters : %r", self.param)

        except requests.exceptions.RequestException as e:
            self._logger.error("HMB connexion error: %s", str(e))
            raise ValueError('Hmb Session not open')

    def _close(self):
        """
        mark session as closed
        """
        self._sid = None

    def info(self):
        """gets info from the hmb server on defined queues, topics and available
        data."""
        try:
            return self._info_request('info')
        except requests.exceptions.RequestException as e:
            self._logger.error("Unable to acces /info: %s", str(e))
            return None

    def features(self):
        """gets functions and capabilities supported by the server and optionally
        the name and version of the server software."""
        try:
            return self._info_request('features')
        except requests.exceptions.RequestException as e:
            self._logger.error("Unable to acces /features: %s", str(e))
            return None

    def status(self):
        """gets status of connected clients (sessions)."""
        try:
            return self._info_request('status')
        except requests.exceptions.RequestException as e:
            self._logger.error("Unable to acces /status: %s", str(e))
            return None

    def _info_request(self, cmd):
        """gets info, functions and capabilities supported by the server."""
        url = self.url + '/' + cmd
        r = self.get_httpsession().get(url, **self.requests_kwargs)
        self._logger.debug('Info %s with status %s', url, r.status_code)
        _check_requests_status_raise(r)

        return r.json()

    def send_msg(self, queue, data, mtype='MSG',
                 topic=None, retries=1):
        """send single message to HMB session.
            mtype - message type (string)
            queue - destination queue of the message
            data - json compatible payload
            topic - optional tag for the message
            retries - number of times to retry sending message
            kwargs - any extra keyvalues to put in the message ie. seq,
                     starttime, endtime
        """
        msg = {"type": mtype,
               "queue": queue,
               "data": data}
        if topic:
            msg["topic"] = topic

        # json messages always require multi-message format
        # bson messages use a different type of concatenation
        if self._use_json:
            msg = {0: msg}

        self.send(msg, retries)

    def _wrap_retry(self, func, args, retries):
        for i in range(retries + 1):
            try:
                if self._sid is None:
                    self._open()
                return func(*args)
            except Exception as e:
                self._close()
                # self._logger.exception('Exception %S with %s, args: %s', str(e), func.__name__, str(args))
                self._logger.error('Exception %s with %s, args: %s', str(e), func.__name__, str(args))
                self._logger.error('HMB retry %s (retries %d/%d)', func.__name__, i, retries)
                time.sleep(self.retry_wait)

        self._logger.error("Max retry: HMB connexion lost")
        raise ValueError('Exit Hmb Session. Max retry reached!')

    def send(self, msg, retries=1):
        """send message to HMB session. Handles disconnections and retries
        sending the message. The message should have the correct hmb format.
        """
        self._wrap_retry(self._send, (msg,), retries)

    def _send(self, msg):
        """actually sends message to HMB session"""
        url = self.url + '/send/' + self._sid
        r = self.get_httpsession().post(
            url,
            headers={"Content-type": "application/json" if self._use_json else "application/bson"},
            data=json.dumps(msg, allow_nan=False) if self._use_json else bson.BSON.encode(msg),
            **self.requests_kwargs)
        self._logger.debug('Send %s with status %s', url, r.status_code)

        _check_requests_status_raise(r)

    def recv_all(self, retries=1, timeout=None):
        """receives all messages from an HMB query. This should not be
        used for realtime operation."""
        starttime = time.time()
        # correction factor so that timeout works as expected.
        starttime -= 0.2
        messages = []
        while True:
            subset = self.recv(retries=retries)
            if not subset:  # no messages received
                pass
            elif subset[-1]['type'] != 'EOF':
                messages += subset
            else:
                messages += subset[:-1]
                break

            if timeout and time.time() > starttime + timeout:
                self._close()
                break
        return messages

    def recv(self, retries=1, keep_heartbeat=False):
        """receives messages from HMB session. Request is blocking until the
        next heartbeat message if "keep=True" is specified in the connection
        parameters for any of the queues.. HEARTBEAT messages are
        elimated but EOF messages are kept so that we know when the end of
        the stream is reached."""

        messages = self._wrap_retry(self._recv, (), retries) or []

        if not keep_heartbeat:
            messages = [m for m in messages if m['type'] not in ('HEARTBEAT', )]
        return messages

    def _recv(self):
        """actually receive messages from HMB. Request is blocking if "keep=True"
        is specified in the connection parameters for any of the queues."""
        url = self.url + '/recv/' + self._sid + self._oid
        r = self.get_httpsession().get(url, **self.requests_kwargs)

        self._logger.debug('Recv %s with status %s', url, r.status_code)

        _check_requests_status_raise(r)

        if self._use_json:
            msgdict = r.json()  # can be multiple messages
            messages = [msgdict[str(i)] for i in range(len(msgdict))]
        else:  # bson
            messages = bson.decode_all(r.content)

        seqnum = None
        for obj in messages:
            # extracts sequence number from messages to ensure future
            # continuity of messages received.
            if 'seq' in obj and 'queue' in obj:
                seqnum = int(obj['seq'])
                if seqnum >= self.param['queue'][obj['queue']]['seq']:
                    self.param['queue'][obj['queue']]['seq'] = seqnum + 1  # next message number
                self._oid = '/%s/%d' % (obj['queue'], seqnum)

        # closing session if EOF message is last message received
        # will always be the last message?
        if len(messages) > 0 and messages[-1]['type'] == 'EOF':
            # self._close()
            return messages[:-1]

        return messages

    def listen(self, callback=generic_hmb_display, delay=0.1, retries=1, keep_heartbeat=False):
        while True:
            try:
                allmsgs = self.recv(retries=retries, keep_heartbeat=keep_heartbeat)
            except KeyboardInterrupt:
                self._logger.warning('Exit HMB Session Listener')
                break
            except Exception:
                self._logger.warning('unexpected exit HMB')
                break

            for msg in allmsgs:
                callback(msg)

            if delay is not None:
                time.sleep(delay)


if __name__ == "__main__":
    from argparse import ArgumentParser
    import os
    sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../'))
    try:
        from configutils import setup_logging
        setup_logging(__name__, startmsg="START")
    except Exception:
        logging.basicConfig(level=logging.INFO)

    argd = ArgumentParser()
    argd.add_argument('action', help='send or listen state', choices=['send', 'listen'])
    argd.add_argument('--bus', help='adresse of the hmb bus')
    argd.add_argument('--timeout', help='define timeout', type=int, default=60)
    argd.add_argument('--backfill', help='Number of messages to backfill', type=int, default=10)
    argd.add_argument('--retry_wait', help='Seconds between retry', type=int, default=10)
    argd.add_argument('--queue', help='define the queue to listen Q1[,Q2[,Q3...]]]', default='SYSTEM_ALERT')
    argd.add_argument('--data', help='txt data to send', default='')
    argd.add_argument('--userpass', '-u', help='user[:pass]')

    args = argd.parse_args()

    queue = args.queue.split(',')

    param = {}
    if args.action == 'listen':
        param = {
            'heartbeat': args.timeout / 2,
            'queue': {}
        }

        for q in queue:
            param['queue'][q] = {
                'seq': -args.backfill - 1,
                'keep': True
            }

    hmbconn = HmbSession(args.bus, param=param, use_bson=True, retry_wait=args.retry_wait)

    if args.userpass is not None:
        tokens = args.userpass.split(':', 1)
        user = tokens[0]
        if len(tokens) == 1:
            mdp = getpass.getpass('Password for {0} : '.format(user))
        else:
            mdp = tokens[1]
        hmbconn.authentication(user, mdp)

    if args.action == 'listen':
        hmbconn.listen(retries=10, keep_heartbeat=False)
    else:
        data = {'content': args.data}
        hmbconn.send_msg('json', args.queue, data)

    hmbconn.close()
