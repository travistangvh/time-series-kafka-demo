# Implement this https://stackoverflow.com/questions/63589249/plotly-dash-display-real-time-data-in-smooth-animation

"""Consumes stream for printing all messages to the console."""

import argparse
from confluent_kafka import Consumer, Producer
from pyspark.sql.functions import *
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from utils import acked, get_consumer_config, get_producer_config, build_spark_session, get_global_config
import logging

import sys
from typing import (
    Union,
    List,
)
from pyspark.sql.window import Window
import pyspark.sql

cfg = get_global_config()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

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
	parser.add_argument('--speed', type=float, default=1, required=False,
						help='Speed up time series by a given multiplicative factor.')

	args = parser.parse_args()

	"""Create producer and consumer and interact with kafka"""
	# Define Kafka consumer configuration
	consumer_conf = get_consumer_config()

	# Define Kafka producer configuration
	producer_conf = get_producer_config()
	producer_conf['kafka.bootstrap.servers'] = producer_conf['bootstrap.servers']
	del producer_conf['bootstrap.servers']

	def ffill(
		target: str,
		partition: Union[str, List[str]],
		sort_key: str,
	) -> pyspark.sql.Column:
		"""
		forward fill

		Args:
			target: column for ffill
			partition: column for partition 
			sort_key: column for sort
		"""
		window = Window.partitionBy(partition) \
			.orderBy(sort_key) \
			.rowsBetween(-sys.maxsize, 0)

		filled_column = last(col(target), ignorenulls=True).over(window)

		return filled_column

	def bfill(
		target: str,
		partition: Union[str, List[str]],
		sort_key: str,
	) -> pyspark.sql.Column:
		"""
		backward fill

		Args:
			target: column for bfill
			partition: column for partition 
			sort_key: column for sort
		"""
		window = Window.partitionBy(partition) \
			.orderBy(sort_key) \
			.rowsBetween(0, sys.maxsize)

		filled_column = first(col(target), ignorenulls=True).over(window)

		return filled_column

	# Define the function to preprocess and send the data to Kafka
	def preprocess_and_send_to_kafka(batch_df, batch_id):
		"""The function is fed into foreachBatch of a writestream.
		It preprocesses the data and sends it to Kafka.
		Thus it should be modified for necessary preprocessing"""

		# Create a new key in the format of "patientid_channelname". 
		# An example of the new key is "p000194_SpO2"
		# The "value" here will be an array containing one value of SpO2 of the patient.
		# .agg(avg("value").alias("average")) \

		batch_df = batch_df\
    	.withColumn(
			"average2",
			ffill(target="average", partition=["key", "channel"], sort_key="windowStart")
		)\
		.withColumn(
			"average3",
			bfill(target="average2", partition=["key", "channel"], sort_key="windowStart")
		)\
		.fillna(0)
		

		preprocessed_df = batch_df.groupby("key","channel")\
			.agg(to_json(collect_list("average3")).alias("value")) \
			.selectExpr(
			'concat(key, "_", channel) as key',
			'value'
		)

		# Logging for debugging purpose
		# logger.info(f'processed len: {preprocessed_df.count()}')
		# logger.info(f'processed: {preprocessed_df.collect()}')

		# This is how preprocessed_df looks like:
		# time-series-kafka-demo-processstream-1  | +--------------------+-------+
		# time-series-kafka-demo-processstream-1  | |                 key|  value|
		# time-series-kafka-demo-processstream-1  | +--------------------+-------+
		# time-series-kafka-demo-processstream-1  | |       p000194_PULSE| [81.0]|
		# time-series-kafka-demo-processstream-1  | |          p000194_HR| [81.0]|
		# time-series-kafka-demo-processstream-1  | |       p000194_PULSE| [80.4]|
		# time-series-kafka-demo-processstream-1  | |       p000194_PULSE| [81.0]|
		# time-series-kafka-demo-processstream-1  | |          p000194_HR| [81.0]|
		# time-series-kafka-demo-processstream-1  | |        p000194_SpO2|[100.0]|
		# time-series-kafka-demo-processstream-1  | |    p000194_NBP Mean|  [0.0]|
		# time-series-kafka-demo-processstream-1  | |        p000194_ST V|  [0.5]|
		# time-series-kafka-demo-processstream-1  | |    p000194_NBP Mean|  [0.0]|
		# time-series-kafka-demo-processstream-1  | |p000194_PVC Rate ...|  [0.0]|
		# time-series-kafka-demo-processstream-1  | |        p000194_ST V|  [0.5]|
		# time-series-kafka-demo-processstream-1  | |    p000194_NBP Dias|  [0.0]|
		# time-series-kafka-demo-processstream-1  | |        p000194_SpO2|[100.0]|
		# time-series-kafka-demo-processstream-1  | |        p000194_ST V|  [0.5]|
		# time-series-kafka-demo-processstream-1  | |          p000194_HR| [81.0]|
		# time-series-kafka-demo-processstream-1  | |    p000194_NBP Mean|  [0.0]|
		# time-series-kafka-demo-processstream-1  | |    p000194_NBP Dias|  [0.0]|
		# time-series-kafka-demo-processstream-1  | |        p000194_RESP| [11.2]|
		# time-series-kafka-demo-processstream-1  | |        p000194_ST V|  [0.5]|
		# time-series-kafka-demo-processstream-1  | |    p000194_NBP Mean|  [0.0]|
		# time-series-kafka-demo-processstream-1  | +--------------------+-------+
		# if preprocessed_df.count()>0:
		# 	logger.info(preprocessed_df.collect())
		# 	raise Exception
		# Send the preprocessed data to Kafka
		preprocessed_df.write.format("kafka")\
			.options(**producer_conf)\
			.option("topic", "call-stream")\
			.option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
			.option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
			.save()

		# For debugging: We can stop the streaming query after a certain number of batches.
		logger.info(f'batch_id: {batch_id}')

		

		# if batch_id >= 10:
		# 	print("Stopping the streaming query...")
		# 	raise Exception

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
								"CAST(REPLACE(replace(substring_index(CAST(value as STRING), ',' ,1),'[',''), '\"', '')  AS INT) as channel",
								"CAST(replace(substring_index(CAST(value as STRING), ',' ,-1),']','') as FLOAT) as value",
								# "date_format(timestamp,'HH:mm:ss') as time",
								"CAST(timestamp as TIMESTAMP) as timestamp")

		# low-pass filtering over l=3 mins 
		# In the actual case, each window has a duration of 3 minutes. The interval between each window is 5 seconds interval.
		# This gives one data point. 
		base_df = base_df.withWatermark("timestamp", f'{(10/args.speed)} seconds') \
		.groupBy(
			base_df.key,
			base_df.channel, 
			window("timestamp", f"{(180/args.speed)} seconds", f'{(5/args.speed)} seconds')) \
		.agg(avg("value").alias("average")) \
		.selectExpr(
			"key"
			,"channel"
			,"window.start as windowStart"
			# ,"window.end"
			,'average'
		)

		# In the last pass filtering, we are obtaining the running average with a window size of 3 minutes (180 seconds).
		# The sliding window is 5 seconds. This means that we are obtaining the average of the last 3 minutes every 5 seconds.
		# Thus, with the passing of 60 seconds, we would have accumulated 60/5=12 additional data points.
		# Since the trigger is 60 seconds, we will be sending 12 data points every time the trigger is pushed.
		kafka_writer: DataStreamWriter = base_df.writeStream.foreachBatch(preprocess_and_send_to_kafka).trigger(processingTime=f'{(60/args.speed)} seconds')

		kafka_query: StreamingQuery = kafka_writer.start()
		
		kafka_query.awaitTermination()

	msg_process(consumer_conf['bootstrap.servers'],
				args.signal_list)

if __name__ == "__main__":
	main()