
# How exchange data via HMB?

HMB is a messaging protocol developped by GFZ [(link)](https://geofon.gfz-potsdam.de/software/httpmsgbus/) allowing the exchange of data on the port 80. To easiest the set up, we have developped some utilities in python. Depending if you want to send or to receive data, you have to use, respectively, a the HMB publisher or the HMB listener.

**MANDATORY. Before establishing the HMB connection, EMSC has to provide an url, a user/password, an agency name and a queue name.** These information can be written in the config file or can be provided on the command line parameters.

Finally, the format of the exchanged data have to be well defined.


## Dependencies
These scripts needs python 3.6+ and libraries requests and pymongo.


## Config file

You can define a config file to set some default parameters and to avoid the usage of passwords in the command line or in scripts.
In this file, it's possible to define:
- agency : so that the EMSC can identify the contributor of the mesage
- url : the url of the HMB server with the bus name
- user : the user for the connexion authentication
- password : the corresponding password

The format of this file (see client.cfg for a template) looks like:
```
agency = ??
url = http://cerf.emsc-csem.org:80/??
user = ??
password = ??
```
You can choose the parameters you want to set in this config file (e.g. only user and password, or only the agency, ...). Note that these parameters can be passed via command line arguments for the publisher/listener and the later overwrites the settings in the config file. Moreover if the user parameter is provided and not the password, the password will be ask.

## HMB Publisher

The HMB publisher is publish_hmb.py. 

```
$ python3 publish_hmb.py -h
usage: publish_hmb.py [-h] [-t {file,fstr,fbin,txt,json}] [--cfg CFG]
                      [--check] [-v] [--url URL] [--queue QUEUE]
                      [--agency AGENCY] [--user USER] [--password PASSWORD]
                      [-m METADATA]
                      msg

positional arguments:
  msg                   filename or txt or json

optional arguments:
  -h, --help            show this help message and exit
  -t {file,fstr,fbin,txt,json}, --type {file,fstr,fbin,txt,json}
                        choose the type of data to send
  --cfg CFG             config file for connexion parameters (e.g. url, queue,
                        agency, user, password)
  --check               skip hmb sending and activate verbose
  -v, --verbose         verbose mode
  --url URL             define the hmb url (server and bus name,
                        http://hmb.server.org/busname)
  --queue QUEUE         define the queue to send the message
  --agency AGENCY       needed in argument or in the --cfg file
  --user USER           connexion authentication
  --password PASSWORD   connexion authentication
  -m METADATA, --metadata METADATA
                        add metadata information to the message. the format is
                        key:val. It can be used multiple times
```

The type of data can be:
- file : to send a file and keep the information of the filename
- fstr : to send only the content of the file as a text
- fbin : same as fstr but send the content as binary
- txt : send a text given as argument
- json : send a json

For instance
```
python3 publish_hmb.py map.png -t file --cfg test/emsc_client.cfg --queue EMSC
python3 publish_hmb.py toto.txt -t fstr --cfg test/emsc_client.cfg --queue EMSC
python3 publish_hmb.py toto.txt -t fbin --cfg test/emsc_client.cfg --queue EMSC
python3 publish_hmb.py "Coucouc ici" -t txt --cfg test/emsc_client.cfg --queue EMSC
python3 publish_hmb.py '{"msg": "send pure python dict", "value": 1, "list": [1, "deux", 3.0]}' -t json --cfg emsc_client.cfg --queue EMSC
```
where map.png is an image and toto.txt a custom text file.

## HMB listener
The listener is listen_hmb.py and loop until user interruption.
At the begining, the listener ask the server to get back the previous nlast messages.
This listener is by default a multithreaded program, however for developement purposes you could consider the option --singlethread to use only one thread.

Since the HMB server works with a heartbeat system and should not be blocked by a long running process, the hmb listener and the message processing are in separate threads.

```
$ python3 listen_hmb.py -h
usage: listen_hmb.py [-h] [--cfg CFG] [--timeout TIMEOUT] [--nlast NLAST]
                     [--queue QUEUE] [--user USER] [--password PASSWORD]
                     [--nthreads NTHREADS] [--singlethread] [-v]
                     url

positional arguments:
  url                  adresse of the hmb bserver

optional arguments:
  -h, --help           show this help message and exit
  --cfg CFG            config file for connexion parameters (e.g. queue, user,
                       password)
  --timeout TIMEOUT    define timeout
  --nlast NLAST        n last message to get backNumber of messages to
                       backfill from the server
  --queue QUEUE        define the queue to listen
  --user USER          connexion authentication
  --password PASSWORD  connexion authentication
  --nthreads NTHREADS  number of concurrent running threads
  --singlethread       force single thread running (useful for debugging)
  -v, --verbose
```

One use example:
    python3 listen_hmb.py cerf.emsc-csem.org:80/EmscProducts --queue FELTREPORTS_0 --cfg test/emsc_client.cfg -v

### Customize the message processing
By default the message processing is defined in the my_processing.py file and the function to edit is the process_message.
This function is launched in another thread and its return is not taking into account.
Note that to launch a shell process in the function, the subprocess.Popen is a good option.

```
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
```

## Python API

### To send data
```
from emschmb import EmscHmbPublisher

agency = 'TOTO'
url = 'http://cerf/emsc-csem.org/hmbtest
user = ??
password = ??

queue = TEST

# Morever it could be usefull to attach metadata to the message
# For instance if the user wants to send a file, and wants make some information directly accessible like event identifier.
# The metadata can be None or a python dictionary... The idea is to choose few parameters... only if needed
# For instance

metadata = {
  'evid': EMSC_EVENT_IDENTIFIER
}

hmb = EmscHmbPublisher(agency, url)
hmb.authentication(user, password)

msg = 'a filename'
hmb.send_file(queue, msg, metadata=metadata)

msg = 'a python string'
hmb.send_str(queue, msg, compress=True, encoding='utf-8', metadata=metadata)

msg = 'python bytes
hmb.send_bin(queue, msg, compress=True, metadata=None)

msg = python object like {"msg": "send pure python dict", "value": 1, "list": [1, "deux", 3.0]}
hmb.send(queue, msg)  # by default metadata = None
```