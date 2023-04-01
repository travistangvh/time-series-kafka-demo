"""Generates a stream to Kafka from a time series csv file."""
import argparse
import json
import time
from dateutil.parser import parse

import wfdb
from confluent_kafka import Producer
from utils import get_global_config, get_producer_config, acked

cfg = get_global_config()

def get_waveform_path(patientid, recordid):
    return cfg['WAVEFPATH'] + f'/{patientid[0:3]}/{patientid}/{recordid}'

def main():
    """Get arguments from command line"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preprocessing-topic', type=str,
                        help='Name of the Kafka topic to receive unprocessed data.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    """Read a patient's waveform data"""
    patient_path = get_waveform_path('p000194', 'p000194-2112-05-23-14-34n')
    record = wfdb.rdrecord(patient_path)
    record_arr = record.__dict__['p_signal'] # Get one of the signals
    frequency = record.fs # sampling frequency in 1/min
    diff = float(frequency*60) # waiting time in second

    """Create producer and send to kafka"""
    producer_conf = get_producer_config()
    producer = Producer(producer_conf)

    for val in record_arr:
        # Read in the records and send over to producer.
        if val[0] is not None: 
            jresult = json.dumps(str(val[0]))
            producer.produce(args.preprocessing_topic, 
                             key='p000194', #Patient ID can be used as the key for the key-value pair. 
                             value=jresult, 
                             callback=acked)
            producer.poll(1)
            producer.flush()
        time.sleep(diff)

if __name__ == "__main__":
    main()