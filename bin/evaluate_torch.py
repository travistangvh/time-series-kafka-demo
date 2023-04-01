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

spark = build_spark_session()

cfg = get_global_config()

def get_waveform_path(patientid, recordid):
    return cfg['WAVEFPATH'] + f'/{patientid[0:3]}/{patientid}/{recordid}'

# def msg_process(server, topic):
#     """Create a streaming dataframe that takes in values of messages received, 
#     together with the current timestamp.
#     Then, print them out.
#     Then, process the message in batch
#     Reference link: https://medium.com/@aman.parmar17/handling-real-time-kafka-data-streams-using-pyspark-8b6616a3a084"""

#     # Print the current time and the message.
#     # time_start = time.strftime("%Y-%m-%d %H:%M:%S")
#     # val = msg.value()
#     # dval = json.loads(val)
#     # print(time_start, dval)
#     # Using this tutorial https://medium.com/@aman.parmar17/handling-real-time-kafka-data-streams-using-pyspark-8b6616a3a084
#     sampleDataframe = (
#             spark.
#             readStream
#             .format("kafka")
#             .option("kafka.bootstrap.servers", server)
#             .option("subscribe", topic)
#             .option("startingOffsets", "latest")
#             .load()
#         )
    
#     # Version 1: Just printing the value
#     base_df = sampleDataframe.selectExpr("CAST(value as STRING)", "timestamp")
    
#     base_df.writeStream.outputMode("append").format("console").start().awaitTermination()

# def produce_result(producer, output_topic, processed_message):
#     # Produce the processed message to the output topic
#     producer.produce(output_topic, value = json.dumps(str(processed_message)), callback=acked)

def run_model():
    print('Starting...')

    patient_path = get_waveform_path('p044083', 'p044083-2112-05-04-19-50n')

    # select only HR from patient path using wfdb
    record = wfdb.rdrecord(patient_path, channel_names=cfg['CHANNEL_NAMES'])
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
    # consumer_conf = {'bootstrap.servers': '172.18.0.4:29092',
    #         'default.topic.config': {'auto.offset.reset': 'smallest'},
    #         'group.id': socket.gethostname()}
    
    # consumer = Consumer(consumer_conf)

    # query = msg_process(consumer_conf['bootstrap.servers'], args.model_call_topic)

    # producer_conf = {
    #     'bootstrap.servers': '172.18.0.4:29092',
    #         'client.id': socket.gethostname(),
    #         'acks':'all' # continuously prints ack every time a message is sent. but slows process down. 
    # }

    # # Create Kafka producer for the output topic
    # producer = Producer(producer_conf)
    consumer_conf = get_consumer_config()
    producer_conf = get_producer_config()
    producer_conf['kafka.bootstrap.servers'] = producer_conf['bootstrap.servers']
    del producer_conf['bootstrap.servers']

    # https://stackoverflow.com/questions/72812187/pythonfailed-to-find-data-source-kafkav
    # spark = SparkSession.builder.appName('Read CSV File into DataFrame').config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()
    # sc = spark.sparkContext
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

        # The model needs to be called
        run_model()

        return query

    query = msg_process(consumer_conf['bootstrap.servers'], args.model_call_topic)
    
    # running = True

    # num_calls = 0 
    # try:
    #     while running:
    #         print('OMG I am here')
    #         consumer.subscribe([args.model_call_topic])

    #         # this script here contains the way to subscribe to multiple topics at the same time:
    #         # https://stackoverflow.com/questions/72021148/subsrcibe-to-many-topics-in-kafka-using-python

    #         msg = consumer.poll(1)
    #         if msg is None:
    #             # print(f'Oops, {msg.topic()} message is None')
    #             continue

    #         if msg.error():
    #             if msg.error().code() == KafkaError._PARTITION_EOF:
    #                 # End of partition event
    #                 sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
    #                                  (msg.topic(), msg.partition(), msg.offset()))
    #             elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
    #                 sys.stderr.write('Topic unknown, creating %s topic\n' %
    #                                  (msg.topic()))
    #             elif msg.error():
    #                 raise KafkaException(msg.error())
    #         else:
    #             # if num_calls ==0:
    #             #     model_response = run_model()
    #             #     producer.produce(args.model_call_topic, pkey = 'patientid', value = json.dumps(str(model_response)), callback=acked)
    #             #     num_calls+=1
    #             # else:
    #             # sys.stderr.write(f'Received message from {msg.topic()}: {msg.value().decode("utf-8")}')
    #             # msg_process(msg, consumer_conf['bootstrap.servers'], args.model_call_topic)

    #             data=msg.value().decode('utf-8')
    #             print(data)
                
    #         model_response = {
    #             'user_id': 'a',
    #             'user_name': 'b'
    #             }
    #         producer.poll(1)
    #         producer.produce(args.model_response_topic, key = 'patientid', value = json.dumps(data).encode('utf-8'), callback=acked)
    #         producer.flush()    
                

    # except KeyboardInterrupt:
    #     query.stop()
    #     pass

    # finally:
    #     # Close down consumer to commit final offsets.
    #     consumer.close()
    #     producer.flush()


if __name__ == "__main__":
    main()
