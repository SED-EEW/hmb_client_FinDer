import json
from subprocess import Popen
import logging


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
            'data': JSON
        }

        where the JSON contains the data.

        json.loads(msg['data']) = {
            'evid': 958332,
            'feltreport': {
                'lon': [22.41761, 22.41241],
                'lat': [39.63461, 39.63714],
                'intensity': [1, 1],
                'dt': [554.0, 244.0]
            }, 
            'eqinfo': {
                'evid': 958332,
                'oritime': '2021-03-11T14:19:40',
                'lon': 22.06,
                'lat': 39.77,
                'magtype': 'mb',
                'mag': 4.5,
                'depth': 4.0,
                'region': 'GREECE',
                'net34': 'INFO',
                'score': 95,
                'eqtxt': 'M4.5 in GREECE\n2021/03/11 14:19:40 UTC'
            }
        }

    Returns:
        subprocess.Popen: the running shell process
    """
    logging.info('begin msg')

    # if you want to see the raw message
    # logging.info(msg)

    creationtime = msg['creationtime']
    agency = msg['agency']  # should be EMSC
    metadata = msg['metadata']  # not used here

    # data is json txt
    data = json.loads(msg['data'])
    logging.info(data)

    evid = data['evid']

    # felt report information
    fdata = data['feltreport']
    lon = fdata['lon']
    lat = fdata['lat']
    intensity = fdata['intensity']
    dt = fdata['dt']

    # event information
    eqinfo = data['eqinfo']
    evlon = eqinfo['lon']
    evlat = eqinfo['lat']
    evdepth = eqinfo['depth']
    evmag = eqinfo['mag']

    logging.info(data)

    # shell command
    # here we juste do nothing for 10 seconds with the shell 'sleep' command
    shellcmd = ['sleep', '10']
    p = Popen(shellcmd)
    return p
