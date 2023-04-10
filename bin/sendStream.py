"""Generates a stream to Kafka from a time series csv file."""
import argparse
import json
import time
from dateutil.parser import parse

import logging
import os
import wfdb
from confluent_kafka import Producer
from utils import get_global_config, get_producer_config, acked

cfg = get_global_config()

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=os.path.join(cfg["DATAPATH"], "producer.log"),
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_waveform_path(patientid, recordid):
    return cfg['WAVEFPATH'] + f'/{patientid[0:3]}/{patientid}/{recordid}'

def main():

    """Read a patient's waveform data"""
    patient_path = get_waveform_path('p044083', 'p044083-2112-05-04-19-50n')
    record = wfdb.rdrecord(patient_path, channel_names=cfg["CHANNEL_NAMES"])
    record_arr = record.__dict__['p_signal'] # Get one of the signals
    record_sig = record.__dict__['sig_name'] # signal list
    frequency = record.fs # sampling frequency in 1/s
    diff = float(1/frequency) # waiting time in second

    """Get arguments from command line"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--signal-list', type=str, nargs="*", default=list(map(lambda x:x.replace(" ","_") ,record_sig)),
                        help='List of the Kafka topic to receive unprocessed data.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    """Create producer and send to kafka"""
    producer_conf = get_producer_config()
    producer = Producer(producer_conf)

    for val in record_arr:
        # Read in the records and send over to producer.
        if val is not None: 
            for i, signal in enumerate(val):

                jresult = json.dumps([record_sig[i], val[i]])

                producer.produce(args.signal_list[i], 
                                key='p000194', #Patient ID can be used as the key for the key-value pair. 
                                value=jresult, 
                                callback=acked)
            
            message = 'Produced message on topic {} with value of {}\n'.format(args.signal_list[i], jresult)
            logger.info(message)
            
        #producer.poll(1)
        producer.flush()
        time.sleep(diff)

if __name__ == "__main__":
    main()