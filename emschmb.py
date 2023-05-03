import sys
import os
from zlib import compress, decompress
import datetime


from hmbsession import HmbSession

__version__ = "1.0"

_genericAuthor = '.'.join((os.path.basename(__file__), __version__))


def _compress_bin(o):
    return compress(o)


def _decompress_bin(o):
    return decompress(o)


def _compress_txt(o, encoding='utf-8'):
    return _compress_bin(o.encode(encoding))


def _decompress_txt(o, encoding='utf-8'):
    return _decompress_bin(o).decode(encoding)


def load_hmbcfg(filename):
    """load a simple config file. the file is formated as some key = val. Comments are lines that starts with a '#'

    Args:
        filename (str): filename of the file containing the configuration

    Returns:
        dict: key, vals parsed in the file
    """
    cfg = {}
    with open(filename, 'r') as f:
        for line in f:
            ll = line.strip()
            if ll.startswith('#'):
                continue
            tokens = ll.split('=', 1)
            if len(tokens) < 2:
                continue

            key, val = tokens

            cfg[key.strip()] = val.strip()

    return cfg


def readstdin():
    while True:
        try:
            line = sys.stdin.readline()
        except KeyboardInterrupt:
            break
        if not line:
            break

        yield line


class EmscHmbPublisher(object):
    """Class to send EMSC message to hmb server.
    It add information on the type of message and allow compression with zlib

    hmb = EmscHmbPublisher('EMSC', 'http://cerf.emsc-csem.org:hmbtest').authentication('user', 'password')

    hmb.send_file : to send file

    hmb.send_str : to send text

    hmb.send_bin : to send byte data

    hmb.send : to send general python object with common types (dict, int, float, list, byte, str)
    """
    def __init__(self, agency, url, author=_genericAuthor, httpsession=False):
        """
        Args:
            agency (str): name of the agency to identify the message
            url (str): full url of the hmt server
            author (str, optional): name of the author. May be usefull to identify the publisher. Defaults to _genericAuthor.
            httpsession (bool, optional): If True don't close the http session after a send. Defaults to False.
        """
        self._use_persistent_httpsession = httpsession
        self._url = url
        self.author = author
        self.agency = agency
        self._requests_args = {}
        self.auth = None

        self._hmb_session = None

    def _get_session(self):
        if self._hmb_session is None:
            self._hmb_session = HmbSession(self._url, use_bson=True, **self._requests_args)
            if self.auth is not None:
                self._hmb_session.authentication(*self.auth)
        return self._hmb_session

    def _header(self, metadata=None):
        header = {
            'creationtime': datetime.datetime.utcnow(),
            'author': self.author,
            'agency': self.agency
        }

        if metadata is not None:
            header['metadata'] = metadata

        return header

    def requests_args(self, **kwargs):
        """possibility to add parameters for requests module

        Returns:
            oject itself
        """
        self._requests_args = kwargs
        return self

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

    def url(self, url):
        """Set the hmb url. It contains the bus name.
        e.g. http://cerf.emsc-csem.org/hmbtest

        Args:
            url (str): full url of the hmt server

        Returns:
            oject itself
        """
        self._url = url
        return self

    def send(self, queue, data, metadata=None):
        """Send a python object with basic types to the queue
        (dict, list, int, float, bool, byte, str)
        add some metadata to the message to avoid the decoding a the whole message to get basic information.

        Args:
            queue (str): queue to send the message
            data (python types): python object to send
            metadata (dict, optional): metadata of the message. Allow additional information to access some data easily. Defaults to None.
        """
        data['_header'] = self._header(metadata=metadata)
        self._get_session().send_msg(queue, data, mtype='EMSC_MSG')
        if not self._use_persistent_httpsession:
            self.close()

    def send_file(self, queue, filename, compress=True, metadata=None):
        """Send the content of a file.

        Args:
            queue (str: queue to send the message
            filename (str): filename of the file to send
            compress (bool, optional): if True use zlib compression. Defaults to True.
            metadata (dict, optional): metadata of the message. Allow additional information to access some data easily. Defaults to None.
        """
        msg = {
            '_type': 'FILE',
            'file': os.path.basename(os.path.abspath(filename)),
            'zlib': compress
        }

        with open(filename, 'rb') as f:
            content = f.read()

        if compress:
            content = _compress_bin(content)
        msg['content'] = content

        self.send(queue, msg, metadata=metadata)

    def send_str(self, queue, txt, encoding='utf-8', compress=True, metadata=None):
        """Send txt.

        Args:
            queue (str): queue to send the message
            txt (str): txt to send
            encoding (str, optional): encoding of the txt. Used if compress is true. Defaults to 'utf-8'.
            compress (bool, optional): if True use zlib compression. Defaults to True.
            metadata (dict, optional): metadata of the message. Allow additional information to access some data easily. Defaults to None.
        """
        msg = {
            '_type': 'STR',
            'encoding': encoding,
            'zlib': compress
        }
        if compress:
            content = _compress_txt(txt, encoding=encoding)
        else:
            content = txt
        msg['content'] = content
        self.send(queue, msg, metadata=metadata)

    def send_bin(self, queue, bin, compress=True, metadata=None):
        """Send bytes

        Args:
            queue (str): queue to send the message
            bin (byte): binary data to send
            compress (bool, optional): if True use zlib compression. Defaults to True.
            metadata (dict, optional): metadata of the message. Allow additional information to access some data easily. Defaults to None.
        """
        msg = {
            '_type': 'BIN',
            'zlib': compress
        }
        if compress:
            content = _compress_bin(bin)
        else:
            content = bin
        msg['content'] = content
        self.send(queue, msg, metadata=metadata)

    def close(self):
        self._get_session().close()


def _decode_file(msg):
    compress = msg.get('zlib', False)
    raw = msg.get('content', b'')
    return {
        'filename': msg.get('file', 'tmp.hmb'),
        'content': _decompress_bin(raw) if compress else raw
    }


def _decode_zstr(msg):
    return _decompress_txt(msg.get('content', b''), encoding=msg.get('encoding', 'utf-8'))


def decode_emsc_msg(rawmsg):
    if rawmsg['type'] != 'EMSC_MSG' or 'data' not in rawmsg:
        return {}

    msg = rawmsg['data']
    header = msg.pop('_header', {}).copy()

    metadata = header.pop('metadata', {})

    res_msg = header
    res_msg['metadata'] = metadata

    msgtype = msg.get('_type', '')

    if msgtype == 'FILE':
        compress = msg.get('zlib', False)
        raw = msg.get('content', b'')
        content = {
            'file': msg.get('file', 'tmp.hmb'),
            'content': _decompress_bin(raw) if compress else raw
        }
        content = _decode_file(msg)
    elif msgtype == 'STR':
        raw = msg.get('content', '')
        compress = msg.get('zlib', False)
        encoding = msg.get('encoding', 'utf-8')
        if isinstance(raw, bytes):
            if compress:
                content = _decompress_txt(raw, encoding=encoding)
            else:
                content = raw
        else:
            content = raw
    elif msgtype == 'BIN':
        raw = msg.get('content', b'')
        compress = msg.get('zlib', False)
        if compress:
            content = _decompress_bin(raw)
        else:
            content = raw
    else:
        content = msg
    res_msg['data'] = content

    return res_msg


class EmscHmbListener(object):
    """Class to listen EMSC message to hmb server.
    It parses automatically some messages to decompress if needed.


    hmb = EmscHmbListener(http://cerf.emsc-csem.org:hmbtest', ['QUEUE1', 'QUEUE2']).authentication('user', 'password')


    """
    def __init__(self, url, queue=(), nlast=10, heartbeat=30):
        """
        Args:
            url (str): queue to send the message
            queue (tuple, optional): queues to listen. Defaults to ().
            nlast (int, optional): number of previous message to get back. Defaults to 10.
            heartbeat (int, optional): define the delay in s for the server heartbeat. Defaults to 30.
        """
        self._url = url
        self._heartbeat = heartbeat
        self._auth = None, None
        self.queue(*queue, nlast=nlast)

    def authentication(self, user, password):
        """Add authentication information

        Args:
            user (str): login username
            password (str): login password

        Returns:
            oject itself
        """
        self._auth = (user, password)
        return self

    def queue(self, *args, nlast=10):
        """set the queues to listen

        Args:
            *args (list of str): queues to listen
            nlast (int, optional): number of previous messages to get back. Defaults to 10.

        Returns:
            oject itself
        """
        res = {}

        for q in args:
            res[q] = {
                'seq': -nlast-1,
                'keep': True
            }
        self._queue = res
        return self

    def get(self, func, queue, filter):
        """"get message on the queue satisfaying filter conditions

        Args:
            func (dict -> None): function to run at each message, that take a dict as argument
            queue (str): queue name
            filter (dict): filtering conditions mongodb format

        """
        param = {
            'heartbeat': self._heartbeat,
        }

        hmb = HmbSession(
            self._url, use_bson=True, retry_wait=10,
            param=param, autocreate_queues=True
        ).authentication(*self._auth).requests_args(timeout=(6.05, self._heartbeat + 5))

        res = []
        for m in hmb.get(queue, filter):
            res.append(func(decode_emsc_msg(m)))

        return res

    def listen(self, func, retries=1):
        """begin the listener and run func for each message

        Args:
            func (dict -> None): function to run at each message, that take a dict as argument
            retries (int, optional): number of retries when the receive failed. Defaults to 1.

        """
        heartbeat = self._heartbeat

        param = {
            'heartbeat': heartbeat,
            'queue': self._queue
        }

        hmb = HmbSession(
            self._url, use_bson=True, retry_wait=10,
            param=param, autocreate_queues=True
        ).authentication(*self._auth).requests_args(timeout=(6.05, heartbeat + 5))

        def func_closure(msg):
            return func(decode_emsc_msg(msg))

        hmb.listen(func_closure, retries=retries, keep_heartbeat=False)

        hmb.close()
