
import logging
import sys
import argparse
import getpass
import json
from emschmb import EmscHmbPublisher, load_hmbcfg


if __name__ == '__main__':
    argd = argparse.ArgumentParser()
    argd.add_argument('msg', help='filename or txt or json')
    argd.add_argument('-t', '--type', help='choose the type of data to send', choices=['file', 'fstr', 'fbin', 'txt', 'json'], default='file')
    argd.add_argument('--cfg', help='config file for connexion parameters (e.g. url, queue, agency, user, password)')
    argd.add_argument('--check', help='skip hmb sending and activate verbose', action='store_true')
    argd.add_argument('-v', '--verbose', help='verbose mode', action='store_true')
    argd.add_argument('--url', help='define the hmb url (server and bus name, http://hmb.server.org/busname)')
    argd.add_argument('--queue', help='define the queue to send the message')
    argd.add_argument('--agency', help='needed in argument or in the --cfg file')
    argd.add_argument('--user', help='connexion authentication')
    argd.add_argument('--password', help='connexion authentication')
    argd.add_argument('-m', '--metadata', help=' add metadata information to the message. the format is key:val. It can be used multiple times',
                      action='append', default=[])

    args = argd.parse_args()
    dargs = vars(args)

    if args.verbose or args.check:
        logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    if args.cfg is not None:
        cfg = load_hmbcfg(args.cfg)
    else:
        cfg = {}

    for k in ['url', 'queue', 'agency', 'user', 'password']:
        if dargs[k] is not None:
            cfg[k] = dargs[k]

    logging.info('Configs : %s', cfg)

    if 'agency' not in cfg:
        raise NameError('Agency is needed, set it with --agency or in the --cfg file')

    metadata = {}
    for items in args.metadata:
        tokens = items.split(':', 1)
        if len(tokens) < 2:
            logging.warning('Skip metadata parsing for %s', items)
            continue
        key, val = tokens
        metadata[key] = val

    if len(metadata) > 0:
        logging.info('Metadata : %s', metadata)
    else:
        metadata = None

    agency = cfg['agency']
    url = cfg['url']
    queue = cfg['queue']

    user = cfg.get('user')
    password = cfg.get('password')

    if user is not None and password is None:
        password = getpass.getpass('Password for {0}'.format(user))

    hmb = EmscHmbPublisher(agency, url)
    if user is not None and password is not None:
        logging.info('Use authentication')
        hmb.authentication(user, password)

    if args.check:
        argd.exit()

    if args.type == 'file':
        hmb.send_file(queue, args.msg, metadata=metadata)
        logging.info('File \'%s\' sent to queue %s', args.msg, args.queue)
    elif args.type == 'fstr':
        with open(args.msg, 'r', encoding='utf-8') as f:
            msg = f.read()
        hmb.send_str(queue, msg, compress=True, encoding='utf-8', metadata=metadata)
        logging.info('Str content of file \'%s\' sent to queue %s', args.msg, args.queue)
    elif args.type == 'fbin':
        with open(args.msg, 'rb') as f:
            msg = f.read()
        hmb.send_bin(queue, msg, compress=True, metadata=metadata)
        logging.info('Binary content of file \'%s\' sent to queue %s', args.msg, args.queue)
    elif args.type == 'txt':
        hmb.send_str(queue, args.msg, compress=False, metadata=metadata)
        logging.info('Txt sent to queue %s', args.queue)
    elif args.type == 'json':
        msg = json.loads(args.msg)
        hmb.send(queue, msg, metadata=metadata)
        logging.info('Json sent to queue %s', args.queue)
    else:
        raise NameError('Not implemented')
