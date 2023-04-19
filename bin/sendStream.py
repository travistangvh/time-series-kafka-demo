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
from threading import Thread
cfg = get_global_config()

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=os.path.join(cfg["DATAPATH"], "producer.log"),
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_waveform_path(recordid):
    patientid = recordid[0:7]
    return cfg['WAVEFPATH'] + f'/{patientid[0:3]}/{patientid}/{recordid}'


def main():
    """Get arguments from command line"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--signal-list', type=str, nargs="*", default=list(map(lambda x:x.replace(" ","_") , cfg['CHANNEL_NAMES'])),
                        help='List of the Kafka topic to receive unprocessed data.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    def send_record_data(record_id):
        """Create producer and send to kafka"""
        producer_conf = get_producer_config()
        producer = Producer(producer_conf)

        """Read a patient's waveform data"""
        patient_path = get_waveform_path(record_id) # e.g. record_id ('p044083-2112-05-04-19-50n')
        record = wfdb.rdrecord(patient_path, channel_names=cfg["CHANNEL_NAMES"])
        record_arr = record.__dict__['p_signal'] # Get one of the signals
        record_sig = record.__dict__['sig_name'] # signal list
        frequency = record.fs # sampling frequency in 1/s
        diff = float(1/frequency) # waiting time in second # augmented with the speed up

        for val in record_arr:
            # Read in the records and send over to producer.
            if val is not None: 
                for i, signal in enumerate(val):

                    # jresult = json.dumps([record_sig[i], val[i]])
                    # We will send the index of record_sig instead of record_sig to reduce the size of the message.
                    jresult = json.dumps([i, val[i]])

                    producer.produce(args.signal_list[i], 
                                    key=record_id[0:7], #Patient ID can be used as the key for the key-value pair. 
                                    value=jresult, 
                                    callback=acked)
                
                # # Comment out to reduce verbosity
                # message = 'Produced message on topic {} with value of {}\n'.format(args.signal_list[i], jresult)
                # logger.info(message)
                
            #producer.poll(1)
            producer.flush()
            time.sleep(diff/args.speed)

    record_id_arr = cfg['PATIENTRECORDS'] # Start the server in a separate thread
    thread_list = [Thread(target=send_record_data, args = [record]) for record in record_id_arr]
    for thread in thread_list:
        thread.start()

    # Wait for threads to collect
    for thread in thread_list:
        thread.join()

if __name__ == "__main__":
    main()