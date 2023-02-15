from subprocess import Popen, PIPE
import logging

logger = logging.getLogger(__name__)


def process_message(msg):
    """The User should modify this function.

    Args:
        msg (dict): dict object sent by the hmb listener.

        For instance:
        msg = {
            'creationtime': datetime.datetime(2021, 3, 11, 15, 5, 36, 695000),
            'author': 'emschmb.py.1.0',
            'agency': 'EMSC',
            'metadata': {},
            'data': CONTENT
        }
    """
    logger.info('begin user processing, metadata %s', msg.get('metadata'))

    # shell command
    # here we juste do nothing for 10 seconds with the shell 'sleep' command
    shellcmd = ['sleep', '10']
    p = Popen(shellcmd, stderr=PIPE, stdout=PIPE)
    stdout, stderr = p.communicate()
