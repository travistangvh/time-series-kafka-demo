import argparse
import wfdb
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import numpy as np
from utils import evaluate, load_dataset
import torch
from torch import nn
from models import MyCNN
from utils import get_global_config, compute_batch_accuracy, compute_batch_auc, acked, get_producer_config, get_consumer_config, build_spark_session

import numpy as np
import pandas as pd
from pyspark.sql.functions import explode, split, from_json, to_json, col, struct

cfg = get_global_config()

def get_waveform_path(patientid, recordid):
    return cfg['WAVEFPATH'] + f'/{patientid[0:3]}/{patientid}/{recordid}'

def run_model():
    """A dummy model that takes uses a dummy model and produces dummy predictions."""
    print('Starting prediction...')

    patient_path = get_waveform_path('p044083', 'p044083-2112-05-04-19-50n')

    # select only HR from patient path using wfdb
    record = wfdb.rdrecord(patient_path, channel_names=cfg['CHANNEL_NAMES'])

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

    model = torch.load(cfg['MODELPATH'])
    print('Model loaded')
    test_dataset = load_dataset(x[1500:],y[1500:])
    test_loader = torch.utils.data.DataLoader(test_dataset, batch_size=1600, shuffle=False, num_workers=cfg['NUM_WORKERS'])
    criterion = nn.CrossEntropyLoss()
    device = torch.device("cuda" if cfg['USE_CUDA'] and torch.cuda.is_available() else "cpu")
    losses_avg, accuracy_avg, results= evaluate(model, device, test_loader, criterion, print_freq=10)

    print(results)
    return results 

def main():
    spark = build_spark_session()

    """Get arguments from command line"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model-call-topic', 
                        type=str,
                        help='Name of the Kafka topic to receive machine learning call.')

    parser.add_argument('--model-response-topic', 
                        type=str,
                        help='Name of the Kafka topic to send the response to the call.')
    
    args = parser.parse_args()

    """Create producer and consumer and interact with kafka"""
    consumer_conf = get_consumer_config()
    producer_conf = get_producer_config()
    producer_conf['kafka.bootstrap.servers'] = producer_conf['bootstrap.servers']
    del producer_conf['bootstrap.servers']

    def preprocess_and_send_to_kafka(batch_df, batch_id):
        """The function is fed into foreachBatch of a writestream.
        It preprocesses the data and sends it to Kafka.
        Thus it should be modified for necessary preprocessing"""

        # Preprocess the data
        # Here it does nothing.
        preprocessed_df = batch_df #.select(lower(col("value")).alias("value_lower"))
        
        # Send the preprocessed data to Kafka
        preprocessed_df.write.format("kafka")\
            .options(**producer_conf)\
            .option("topic", "response-stream")\
            .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
            .option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
            .save()

    def msg_process(server, topic):
        """Create a streaming dataframe that takes in values of messages received, 
        together with the current timestamp.
        Then, print them out.
        Then, process the message in batch
        Reference link: https://medium.com/@aman.parmar17/handling-real-time-kafka-data-streams-using-pyspark-8b6616a3a084"""

        df = (spark.
                readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .load()
            )
        
        # Select the value and timestamp (the message is received)
        base_df = df.selectExpr("CAST(value as STRING)", "timestamp")
        
        # to see what "base_df" is like in the stream,
        # Uncomment base_df.writeStream.outputMode(...)
        # and comment out base_df.writeStream.foreachBatch(...)
        query = base_df.writeStream.outputMode("append").format("console").trigger(processingTime='10 seconds').start()
        query.awaitTermination()

        # Write the preprocessed DataFrame to Kafka in batches.
        # kafka_writer: DataStreamWriter = base_df.writeStream.foreachBatch(preprocess_and_send_to_kafka)
        # kafka_query: StreamingQuery = kafka_writer.start()
        # kafka_query.awaitTermination()

        # The model needs to be called to store the data
        run_model()

        return query

    query = msg_process(consumer_conf['bootstrap.servers'], args.model_call_topic)

if __name__ == "__main__":
    main()