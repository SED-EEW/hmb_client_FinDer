import json
from subprocess import call, Popen, PIPE
import logging,numpy,os,shutil
import datetime

from emschmb import EmscHmbPublisher, load_hmbcfg

def I_Allen2012_Rhypo(eq_mag,
                      eq_depth,
                      sta_dist,
                      c0 =  2.085,
                      c1 =  1.428,
                      c2 = -1.402,
                      c4 =  0.078,
                      m1 = -0.209,
                      m2 =  2.042,
                      Imin = 3):

    RM = m1 + m2*numpy.exp(eq_mag-5)
    R_hypo = numpy.sqrt(eq_depth**2+sta_dist**2)
    I_sim   = c0 + (c1 * eq_mag) + c2*numpy.log(numpy.sqrt(R_hypo**2+RM**2))+c4*numpy.log(R_hypo/50)
    idx = (R_hypo<=50)
    I_sim[idx] = c0 + (c1 * eq_mag) + c2*numpy.log(numpy.sqrt(R_hypo[idx]**2+RM**2))
    max_dist = numpy.sqrt((numpy.exp((Imin-c0-(c1 * eq_mag)-c4*numpy.log(R_hypo/50))/c2))**2-RM**2)

    return I_sim, max_dist

def I_to_PGA_Wordon2012(sta_I,
                        alpha1 =  1.78,
                        beta1  =  1.557,
                        alpha2 = -1.60,
                        beta2  =  3.7,
                        thres  =  4.22):

    sta_logPGA = [0 for i in sta_I]
    for i in range(len(sta_I)):
        if sta_I[i]<=thres :
            sta_logPGA[i] = (sta_I[i]-alpha1)/beta1
        else:
            sta_logPGA[i] = (sta_I[i]-alpha2)/beta2

    return sta_logPGA

def haversine(lon1, lat1, lon2, lat2,
              r = 6371 # Radius of earth in kilometers. Use 3956 for miles
              ):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    lon1, lat1, lon2, lat2 = map(numpy.deg2rad, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = numpy.sin(dlat/2)**2 + numpy.cos(lat1) * numpy.cos(lat2) * numpy.sin(dlon/2)**2
    c = 2 * numpy.arcsin(numpy.sqrt(a))

    return c * r

def process_message(msg,
                    epicenter=    '/project/Results_from_FinDer_for_EMSC_felt_reports/finder_inputs/config/gmt_input/epicenter.txt',
                    focmec=       '/project/Results_from_FinDer_for_EMSC_felt_reports/finder_inputs/config/gmt_input/focmec.txt',
                    finder_run=   '/home/maboese/FinDer_EMSC/finder_file/finder_run',                         # fullpath to finder_run
                    finder_conf=  '/project/Results_from_FinDer_for_EMSC_felt_reports/finder_inputs/config/', # path to finder_run config files directory
                    finder_inputs='/project/Results_from_FinDer_for_EMSC_felt_reports/finder_inputs/',        # path to finder inputs file directory
                    finder_logs=  '/project/Results_from_FinDer_for_EMSC_felt_reports/finder_logs/',          # path to finder log file directory
                    S=0.25,
                    publish=True,
                    **pubopt):
    """The User should modify this function.

    Args:
        msg (dict): dict object sent by the hmb listener.

        For instance:
        msg = {
            'creationtime': datetime.datetime(2021, 3, 11, 15, 5, 36, 695000),
            'author': 'emschmb.py.1.0',
            'agency': 'EMSC',
            'metadata': {"tag" : "t0+60min", # a short description of the message, here the time of publication
                         "count" : 4,        # number of message sent for this evid (it begins at 1, it may be more consistent if it begins at 0...)
                         "evid" : 980032,    # the EMSC event identifier
                         "version" : 3},     # identify the message. 0 is for T0+10,... 3 for T0+60 et 4 for T0+120.
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
    """
    logging.info('begin msg')

    # if you want to see the raw message
    # logging.info(msg)

    creationtime = msg['creationtime']
    agency = msg['agency']  # should be EMSC
    metadata = msg['metadata']
    version = metadata['version']
    count = metadata['count']

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

    #logging.info(data)
   
    #(1) Remove unrealistic high intensities (>10), reports at large distance (max_dist) and delta_I>3 compared to Allen et al. (2012):
    data_distances = haversine(evlon, evlat, lon, lat)
    I_sim, max_dist = I_Allen2012_Rhypo(evmag, evdepth, data_distances)
    realistic_data_mask = (numpy.round(intensity)<=10) * (data_distances<max_dist) * (numpy.abs(intensity-I_sim)<=3)
    logging.info('Filtering %d realistic felt reports from %d total'%(sum(realistic_data_mask),len(realistic_data_mask)))

    #(2) Add artificial intensity datapoint at epicenter:
    epicentral_intensity = I_Allen2012_Rhypo(evmag, evdepth, numpy.asarray([0.001]))[0][0]+S
    logging.info('Epicentral intensity: %s (M%.1f, %.1f km bsl)'%(epicentral_intensity, evmag, evdepth))
    
    #(3) Convert intensity to PGA:
    logPGA = I_to_PGA_Wordon2012(intensity)
    logging.info('Intensities: %s'%', '.join(['%.1f'%d for d in list(set(intensity))]))
    logging.info('Wordon (2012) log(PGA): %s'%', '.join(['%.4f'%d for d in list(set(logPGA))]))

    #(4) Produce FinDer input files <path>/data_0:
    #    e.i., 3 columns with: lat lon pga_in_cm**2
    finder_data  = [[lat[i], lon[i], logPGA[i]] for i in numpy.where(realistic_data_mask>0)[0]] 
    finder_data += [[ evlat,  evlon, I_to_PGA_Wordon2012([epicentral_intensity])[0] ]]
    
    if not os.path.exists('%s/%s'%(finder_inputs,evid)):
        os.makedirs('%s/%s'%(finder_inputs,evid))
    logs = 'Writing version %d inputs in %s/%s/data_%d:\n'%(version,finder_inputs,evid,count)
    logged = False
    with open('%s/%s/data_%d'%(finder_inputs,evid,count), 'w') as f: 
        for d in finder_data:
            towrite = '%s %s %s\n'%tuple(d)
            f.write(towrite) 
            if not logged:
                logs+=towrite
            logged=True
    logging.info('%s...\n%s'%(logs,towrite))
    shutil.copyfile('%s/%s/data_%d'%(finder_inputs,evid,count), '%s/%s/data_0'%(finder_inputs,evid))

    # (4.1) make epicenter input for the ps file
    with open(epicenter, 'w') as f:
        towrite = '%s\t%s\n'%(evlon,evlat)
        f.write(towrite)
    logging.info('Writing epicenter in %s:\n%s'%(epicenter,towrite))

    #(5) Call FinDer (in finder_file/), config depends on rounded event magnitude: 
    #    e.g., ./finder_run finder_socialmedia_M<int(round(mag*10))>.config <path>/ 0 0 no > log
    shellcmd = [finder_run, 
                '%sfinder_socialmedia_M%s.config'%(finder_conf, int(numpy.round(evmag*10))),
                '%s/%s'%(finder_inputs,evid),
                '%d'%(count), #'v%d.c%d'%(version,count), # '0', # update number to start with (there must be a data_N)
                '%d'%(count), #'v%d.c%d'%(version,count), # '0', # update number to end with (there must be a data_N)
                'no' # 
                ] 
    logging.info('Running:\n%s'%' '.join(shellcmd))

    # shell command
    #shellcmd = ['sleep', '10'] # here we juste do nothing for 10 seconds with the shell 'sleep' command
    if not os.path.exists(finder_logs):
        os.makedirs(finder_logs)
    
    now = int((datetime.datetime.now() - datetime.datetime(1970,1,1)).total_seconds())
    logfname = '%s%s.%s.stderrout'%(finder_logs,evid,now)
    with open(logfname, 'w') as logf:
        p = call(shellcmd, stderr=logf, stdout=logf)
        #p = Popen(shellcmd, stderr=logf, stdout=logf)
        #p.wait()
        #stdout, stderr = p.communicate()
        #logging.info(stdout)
        #logging.info(stderr)

    
    with open(logfname) as logf:
        logs = logf.read().splitlines()
        psfnameguess = list(set([e for l in logs for e in l.split(' ') if ".ps"==e[-3:]]))

    if len(psfnameguess)==1:
        logging.info('The ouput file is %s'%(psfnameguess[0]))
        #call(['convert',psfnameguess[0], psfnameguess[0].replace('.ps','.pdf')], stderr=logf, stdout=logf)
        #logging.info('... converted to %s'%(psfnameguess[0].replace('.ps','.pdf')))
         
        if (publish 
            and 'agency' in pubopt 
            and 'url' in pubopt
            and 'user' in pubopt
            and 'password' in pubopt
            and 'queue_pub' in pubopt):
            logging.info('--------------------- PUBLISHING -------------------')
            hmb = EmscHmbPublisher(pubopt['agency'], pubopt['url'])
            hmb.authentication(pubopt['user'], pubopt['password'])
            hmb.send_file(pubopt['queue_pub'], psfnameguess[0], metadata=metadata)
            logging.info('--------------------- DONE PUBLISHING -------------------')

    elif len(psfnameguess)>1:
        logging.info('WARNING !! Found several ouput files: %s'%(', '.join(psfnameguess)))
    #else:
    #    logging.info('WARNING !! No file found in')
    #    for log in logs:
    #        print(log)
    """
    Fred's todo list:
    - [X] ask & implement update #
    - ask & implement [X] epicenter [ ] focal mec
    - improve filenames
    - add logo, creation time, signature in ps files
    """

    """
    # note that it would be usefull to add metadata information
    # to associate the result with the corresponding EMSC event

        'evid': evid
    }

    # Publish to result to EMSC HMB server
    hmb = EmscHmbPublisher(agency, url)
    hmb.authentication(user, password)
    hmb.send_file(queue, args.msg, metadata=metadata)
    """
