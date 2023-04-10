"""Consumes stream for printing all messages to the console."""

import argparse
from confluent_kafka import Consumer, Producer
from pyspark.sql.functions import *
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from utils import acked, get_consumer_config, get_producer_config, build_spark_session, get_global_config

cfg = get_global_config()

def main():
    """Create SparkSession.
    Explanation on why the .config(spark.jars.packages) is needed: 
    https://stackoverflow.com/questions/72812187/pythonfailed-to-find-data-source-kafkav"""
    spark = build_spark_session()

    """Get arguments from command line"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--signal-list', 
                        type=str,
                        nargs="*",
                        default=list(map(lambda x:x.replace(" ","_"), cfg["CHANNEL_NAMES"])),
                        help='List of the Kafka topic to receive unprocessed data.')
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

<<<<<<< Updated upstream
        # select the data
        # value should be string
        # preprocessed_df = batch_df.selectExpr("CAST(average as STRING) as value")

        
        preprocessed_df = batch_df.groupby("key","start","end")\
            .agg(to_json(collect_list(struct("channel","average"))).alias("value")) \
            .selectExpr(
            'key',
            'value'
        )
        
        
        # Send the preprocessed data to Kafka
        preprocessed_df.write.format("kafka")\
            .options(**producer_conf)\
            .option("topic", "call-stream")\
            .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
            .option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
            .save()

    def msg_process(server, topics):
        """Create a streaming dataframe that takes in values of messages received, 
        together with the current timestamp.
        Then, print them out.
        Then, process the message in batch
        Reference link: https://medium.com/@aman.parmar17/handling-real-time-kafka-data-streams-using-pyspark-8b6616a3a084"""

        df = (spark.
                readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", ",".join(topics))
                .option("startingOffsets", "latest")
                .load()
            )
        
        # Select the value and timestamp (the message is received)

        base_df = df.selectExpr("CAST(key as STRING) as key",
                                "CAST(replace(substring_index(CAST(value as STRING), ',' ,1),'[','') as STRING) as channel",
                                "CAST(replace(substring_index(CAST(value as STRING), ',' ,-1),']','') as FLOAT) as value",
                                "date_format(timestamp,'HH:mm:ss') as time",
                                "CAST(timestamp as TIMESTAMP) as timestamp")\
                    .fillna(0) 
        
        # low-pass filtering over l=3 mins
        base_df = base_df.withWatermark("timestamp", "3 minutes") \
        .groupBy(
            base_df.key,
            base_df.channel,
            window("timestamp", "3 minutes", '5 seconds')) \
        .agg(avg("value").alias("average")) \
        .selectExpr(
            "key"
            ,"replace(channel, '\"', '') as channel"
            ,"window.start"
            ,"window.end"
            ,'average'
        )
        
        # to see what "base_df" is like in the stream,
        # Uncomment base_df.writeStream.outputMode(...)
        # and comment out base_df.writeStream.foreachBatch(...)
        # query = base_df.writeStream.outputMode("append").format("console").trigger(processingTime='10 seconds').start()
        # query.awaitTermination()

        # Write the preprocessed DataFrame to Kafka in batches.
        
        kafka_writer: DataStreamWriter = base_df.writeStream \
        .foreachBatch(preprocess_and_send_to_kafka)

        kafka_query: StreamingQuery = kafka_writer.start()
        
        print("await termination")
        kafka_query.awaitTermination()
        
=======
		# Create a new key in the format of "patientid_channelname". 
		# An example of the new key is "p000194_SpO2"
		# The "value" here will be an array containing one value of SpO2 of the patient.
		preprocessed_df = batch_df.groupby("key","channel","start","end")\
			.agg(to_json(collect_list("average")).alias("value")) \
			.selectExpr(
			'concat(key, "_", channel) as key',
			'value'
		)

		# print out preprocessed_df
		preprocessed_df.show()
		
		# Send the preprocessed data to Kafka
		preprocessed_df.write.format("kafka")\
			.options(**producer_conf)\
			.option("topic", "call-stream")\
			.option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
			.option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
			.save()

	def msg_process(server, topics):
		"""Create a streaming dataframe that takes in values of messages received, 
		together with the current timestamp.
		Then, print them out.
		Then, process the message in batch
		Reference link: https://medium.com/@aman.parmar17/handling-real-time-kafka-data-streams-using-pyspark-8b6616a3a084"""

		df = (spark.
				readStream
				.format("kafka")
				.option("kafka.bootstrap.servers", server)
				.option("subscribe", ",".join(topics))
				.option("startingOffsets", "latest")
				.load()
			)
		
		# Select the value and timestamp (the message is received)

		base_df = df.selectExpr("CAST(key as STRING) as key",
								"CAST(replace(substring_index(CAST(value as STRING), ',' ,1),'[','') as STRING) as channel",
								"CAST(replace(substring_index(CAST(value as STRING), ',' ,-1),']','') as FLOAT) as value",
								"date_format(timestamp,'HH:mm:ss') as time",
								"CAST(timestamp as TIMESTAMP) as timestamp")\
					.fillna(0) 

		# low-pass filtering over l=3 mins 
		# In the actual case, each window has a duration of 3 minutes. The interval between each window is 5 seconds interval.
		# In the test case, each window has a duration of 5 seconds. The interval between each window is 2 seconds interval
		base_df = base_df.withWatermark("timestamp", "3 seconds") \
		.groupBy(
			base_df.key,
            base_df.channel,
			window("timestamp", "5 seconds", '2 seconds')) \
		.agg(avg("value").alias("average")) \
		.selectExpr(
			"key"
			,"replace(channel, '\"', '') as channel"
			,"window.start"
			,"window.end"
			,'average'
		)
		
		# To see what "base_df" is like in the stream,
		# Uncomment base_df.writeStream.outputMode(...)
		# and comment out base_df.writeStream.foreachBatch(...)
		# query = base_df.writeStream.outputMode("append").format("console").trigger(processingTime='10 seconds').start()
		# query.awaitTermination()

		# Write the preprocessed DataFrame to Kafka in batches.
		kafka_writer: DataStreamWriter = base_df.writeStream.foreachBatch(preprocess_and_send_to_kafka)

		kafka_query: StreamingQuery = kafka_writer.start()
		
		kafka_query.awaitTermination()
		
>>>>>>> Stashed changes

    msg_process(consumer_conf['bootstrap.servers'],
                args.signal_list)

if __name__ == "__main__":
    main()