import os
import argparse
import json
import sys
import time
import socket
import wfdb
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

# from pyspark.sql import SparkSession
# from pyspark.pandas.frame import DataFrame
import numpy as np
import pandas as pd
# from pyspark.sql.functions import explode, split, from_json, to_json, col, struct
# from pyspark.sql.types import StringType, StructType
from utils import evaluate, load_dataset
import torch
from torch import nn
from models import MyCNN
import configparser

config = configparser.ConfigParser()
config.read('config.cfg')

from pyspark.sql import SparkSession
from pyspark.pandas.frame import DataFrame
import numpy as np
import pandas as pd
from pyspark.sql.functions import explode, split, from_json, to_json, col, struct
from pyspark.sql.types import StringType, StructType

spark = SparkSession.builder.appName('Read CSV File into DataFrame').config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()
sc = spark.sparkContext

MOUNTPATH = config.get('PATHS', 'MOUNTPATH')
DATAPATH = config.get('PATHS', 'DATAPATH')
MIMICPATH = config.get('PATHS', 'MIMICPATH')
DEMOPATH = config.get('PATHS', 'DEMOPATH')
WAVEFPATH = config.get('PATHS', 'WAVEFPATH')
OUTPUTPATH = config.get('PATHS', 'OUTPUTPATH')
MODELPATH = config.get('PATHS', 'MODELPATH')

USE_CUDA = config.getboolean('SETTINGS', 'USE_CUDA')
NUM_WORKERS = config.getint('SETTINGS', 'NUM_WORKERS')
CHANNEL_NAMES = config.get('SETTINGS', 'CHANNEL_NAMES').split(', ')


def get_waveform_path(patientid, recordid):
    return WAVEFPATH + f'/{patientid[0:3]}/{patientid}/{recordid}'

# https://stackoverflow.com/questions/72812187/pythonfailed-to-find-data-source-kafkav
# spark = SparkSession.builder.appName('Read CSV File into DataFrame').config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()
# sc = spark.sparkContext

def msg_process(server, topic):
    # Print the current time and the message.
    # time_start = time.strftime("%Y-%m-%d %H:%M:%S")
    # val = msg.value()
    # dval = json.loads(val)
    # print(time_start, dval)
    # Using this tutorial https://medium.com/@aman.parmar17/handling-real-time-kafka-data-streams-using-pyspark-8b6616a3a084
    sampleDataframe = (
            spark.
            readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", server)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
        )
    
    # Version 1: Just printing the value
    base_df = sampleDataframe.selectExpr("CAST(value as STRING)", "timestamp")
    
    base_df.writeStream.outputMode("append").format("console").start().awaitTermination()


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))

def produce_result(producer, output_topic, processed_message):
    # Produce the processed message to the output topic
    producer.produce(output_topic, value = json.dumps(str(processed_message)), callback=acked)

def get_data():
    return 

def run_model():
    print('Starting...')

    patient_path = get_waveform_path('p044083', 'p044083-2112-05-04-19-50n')

    # select only HR from patient path using wfdb
    record = wfdb.rdrecord(patient_path, channel_names=CHANNEL_NAMES)
    # record = wfdb.rdrecord(patient_path) 

    record_df = record.to_dataframe()
    record_df = record_df.interpolate(method='linear').fillna(0)

    # We take ten minutes of data with 1 minute of sliding window
    record_df = record_df.iloc[::5, :]

    # Which means that each should have 120 records
    record_arr = np.stack([record_df.iloc[i:i+int(600/5), :].values for i in range(0, len(record_df)-int(600/5), int(60/5))])
    record_arr.shape
    record_arr = record_arr.reshape(2739, 7, 120)
    record_arr

    x = record_arr
    record_df['y']=1
    y = record_df['y'].astype(np.float32).values
    y = np.random.randint(low=0, high=2, size=record_arr.shape[0]).squeeze()
    y.shape

    model = torch.load(MODELPATH)
    print('Model loaded')
    test_dataset = load_dataset(x[1500:],y[1500:])
    test_loader = torch.utils.data.DataLoader(test_dataset, batch_size=1600, shuffle=False, num_workers=NUM_WORKERS)
    criterion = nn.CrossEntropyLoss()
    device = torch.device("cuda" if USE_CUDA and torch.cuda.is_available() else "cpu")
    losses_avg, accuracy_avg, results= evaluate(model, device, test_loader, criterion, print_freq=10)

    print(results)
    return results 

def main():

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model-call-topic', 
                        type=str,
                        help='Name of the Kafka topic to receive machine learning call.')

    parser.add_argument('--model-response-topic', 
                        type=str,
                        help='Name of the Kafka topic to send the response to the call.')
    args = parser.parse_args()

    consumer_conf = {'bootstrap.servers': '172.18.0.4:29092',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': socket.gethostname()}

    consumer = Consumer(consumer_conf)

    query = msg_process(consumer_conf['bootstrap.servers'], args.model_call_topic)

    producer_conf = {
        'bootstrap.servers': '172.18.0.4:29092',
            'client.id': socket.gethostname(),
            'acks':'all' # continuously prints ack every time a message is sent. but slows process down. 
    }

    # Create Kafka producer for the output topic
    producer = Producer(producer_conf)

    running = True

    num_calls = 0 
    try:
        while running:
            print('OMG I am here')
            consumer.subscribe([args.model_call_topic])

            # this script here contains the way to subscribe to multiple topics at the same time:
            # https://stackoverflow.com/questions/72021148/subsrcibe-to-many-topics-in-kafka-using-python

            msg = consumer.poll(1)
            if msg is None:
                # print(f'Oops, {msg.topic()} message is None')
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write('Topic unknown, creating %s topic\n' %
                                     (msg.topic()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # if num_calls ==0:
                #     model_response = run_model()
                #     producer.produce(args.model_call_topic, pkey = 'patientid', value = json.dumps(str(model_response)), callback=acked)
                #     num_calls+=1
                # else:
                # sys.stderr.write(f'Received message from {msg.topic()}: {msg.value().decode("utf-8")}')
                # msg_process(msg, consumer_conf['bootstrap.servers'], args.model_call_topic)

                data=msg.value().decode('utf-8')
                print(data)
                
            model_response = {
                'user_id': 'a',
                'user_name': 'b'
                }
            producer.poll(1)
            producer.produce(args.model_response_topic, key = 'patientid', value = json.dumps(data).encode('utf-8'), callback=acked)
            producer.flush()    
                

    except KeyboardInterrupt:
        query.stop()
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
