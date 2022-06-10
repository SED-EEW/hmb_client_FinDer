import json
from subprocess import Popen, PIPE
import logging

# from emschmb import EmscHmbPublisher, load_hmbcfg


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
    logging.info('begin user processing')

    # shell command
    # here we juste do nothing for 10 seconds with the shell 'sleep' command
    shellcmd = ['sleep', '10']
    p = Popen(shellcmd, stderr=PIPE, stdout=PIPE)
    stdout, stderr = p.communicate()
