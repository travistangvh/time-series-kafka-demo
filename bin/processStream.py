"""Consumes stream for printing all messages to the console."""

import argparse
from confluent_kafka import Consumer, Producer
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
from pyspark.sql.functions import explode, split, from_json, to_json, col, struct, lower
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from utils import acked, get_consumer_config, get_producer_config

def main():
    """Create SparkSession.
    Explanation on why the .config(spark.jars.packages) is needed: 
    https://stackoverflow.com/questions/72812187/pythonfailed-to-find-data-source-kafkav"""
    spark = SparkSession.builder.appName('Read CSV File into DataFrame').config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()

    """Get arguments from command line"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preprocessing-topic', 
                        type=str,
                        help='Name of the Kafka topic to receive unprocessed data.')
    parser.add_argument('--model-call-topic', 
                        type=str, 
                        help='Name of the Kafka topic to send preprocessed data to machine learning model.')
    parser.add_argument('--model-response-topic', 
                        type=str, 
                        help='Name of the Kafka topic to receive response from machine learning model.')

    args = parser.parse_args()

    """Create producer and consumer and interact with kafka"""
    # Define Kafka consumer configuration
    consumer_conf = get_consumer_config()

    # Define Kafka producer configuration
    producer_conf = get_producer_config()
    producer_conf['kafka.bootstrap.servers'] = producer_conf['bootstrap.servers']
    del producer_conf['bootstrap.servers']

    # Define the function to preprocess and send the data to Kafka
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
            .option("topic", "call-stream")\
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
        
        # Uncomment this line to see the messages received in the terminal.
        query = base_df.writeStream.outputMode("append").format("console").trigger(processingTime='10 seconds').start()

        # Write the preprocessed DataFrame to Kafka in batches.
        kafka_writer: DataStreamWriter = base_df.writeStream.foreachBatch(preprocess_and_send_to_kafka)
        kafka_query: StreamingQuery = kafka_writer.start()

        kafka_query.awaitTermination()


    msg_process(consumer_conf['bootstrap.servers'], 
                args.preprocessing_topic)

if __name__ == "__main__":
    main()


"""Some drafts that can be removed from before.
    # running = True

    # try:
    #     while running:
    #         consumer.subscribe([args.preprocessing_topic, args.model_response_topic])

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
    #                 break
    #         else:
    #             pass
    #             # sys.stderr.write(f'Received message from {msg.topic()}: {msg.value().decode("utf-8")}')
    #             # This works, but commenting out because this is not the pattern that we want
    #             # data={
    #             #     'user_id': 'a',
    #             #     'user_name': 'b'
    #             #     }
    #             # producer.produce(args.model_call_topic, key = 'patientid', value = json.dumps(data).encode('utf-8'), callback=acked)
    #             # producer.poll(1)

    #             # # Flush any pending messages in the producer buffer
    #             # producer.flush()
    #             # End this works
    # except KeyboardInterrupt:
    #     query.stop()
    #     pass

    # finally:
    #     # Close down consumer to commit final offsets.
    #     consumer.close()
    #     producer.flush()
"""