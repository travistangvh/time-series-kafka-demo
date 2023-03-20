#!/usr/bin/env python
# run the following:
# docker compose run 
# conda activate bd4hproject
# python bin/sendStream.py my-stream
"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket
import pandas as pd
import wfdb

DATAPATH = '/Users/michaelscott/bd4h/project/data' 
WAVEFPATH = DATAPATH + '/waveform/physionet.org/files/mimic3wdb-matched/1.0'

def get_waveform_path(patientid, recordid):
    return WAVEFPATH + f'/{patientid[0:3]}/{patientid}/{recordid}'

patient_path = get_waveform_path('p000194', 'p000194-2112-05-23-14-34n')
record = wfdb.rdrecord(patient_path)
record_arr = record.__dict__['p_signal']
frequency = record.fs # sampling frequency in 1/min
diff = float(frequency*60) # waiting time in second

# wfdb.plot_wfdb(record=record, title='Record a103l from PhysioNet Challenge 2015') 
# display(record.__dict__)
# record_df = pd.DataFrame(record.__dict__['p_signal'])

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
    
    parser = argparse.ArgumentParser(description=__doc__)
    # parser.add_argument('filename', type=str,
    #                     help='Time series csv file.')
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = "stg" # args.filename

    conf = {'bootstrap.servers': "localhost:29092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    # rdr = csv.reader(open(args.filename))
    # next(rdr)  # Skip header
    firstline = True

    # while True:

        # try:

            # if firstline is True:
            #     line1 = next(rdr, None)
            #     timestamp, value = line1[0], float(line1[1])
            #     # Convert csv columns to key value pair
            #     result = {}
            #     result[timestamp] = value
            #     # Convert dict to json as message format
            #     jresult = json.dumps(result)
            #     firstline = False

            #     producer.produce(topic, key=p_key, value=jresult, callback=acked)

            # else:
            #     line = next(rdr, None)
            #     # d1 = parse(timestamp)
            #     # d2 = parse(line[0])
            #     # diff = ((d2 - d1).total_seconds())/args.speed
            #     time.sleep(diff)
            #     timestamp, value = line[0], float(line[1])
            #     result = {}
            #     result[timestamp] = value
            #     jresult = json.dumps(result)
            #     producer.produce(topic, key=p_key, value=jresult, callback=acked)

    for idx,val in enumerate(record_arr):
        # d1 = parse(timestamp)
        # d2 = parse(line[0])
        # diff = ((d2 - d1).total_seconds())/args.speed
        print(val)
        print(val[0])
        time.sleep(diff)
        # timestamp, value = record_arr[i], float(line[1])
        # result = {}
        # result[timestamp] = value
        # To do: add different values
        # To do: add name
        if val[0] is not None: 
            jresult = json.dumps(str(val[0]))
            producer.produce(topic, key=p_key, value=jresult, callback=acked)

    producer.flush()

        # except TypeError:
        #     sys.exit()

if __name__ == "__main__":
    main()