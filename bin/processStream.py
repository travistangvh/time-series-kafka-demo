"""Consumes stream for printing all messages to the console."""

import argparse
from confluent_kafka import Consumer, Producer
from pyspark.sql.functions import explode, split, from_json, to_json, col, struct, lower, array
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from utils import acked, get_consumer_config, get_producer_config, build_spark_session, get_global_config
from pyspark.sql.functions import lit
from pyspark.sql.functions import concat, to_json, struct, lit, col
def main():
	"""Create SparkSession.
	Explanation on why the .config(spark.jars.packages) is needed: 
	https://stackoverflow.com/questions/72812187/pythonfailed-to-find-data-source-kafkav"""
	spark = build_spark_session()
	cfg = get_global_config()

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


	# mock_df = spark.read.csv(F'file://{cfg["EXPLOREPATH"]}/X.TESTINPUT', header=True, inferSchema=True)
	# new_col = to_json(struct(
	# 			array(mock_df['HR']),
	# 			array(mock_df['RESP']),
	# 			array(mock_df['PULSE']),
	# 			array(mock_df['etco2']),
	# 			array(mock_df['SpO2']),
	# 			array(mock_df['CVP']),
	# 			array(mock_df['AWRR']),
	# 			array(mock_df['NBP_Mean']),
	# 			array(mock_df['NBP_Dias']),
	# 			array(mock_df['NBP_Sys'])
	# 		))
	# new_col = to_json(struct(
	# 			array(mock_df['value']['HR']),
	# 			array(mock_df['value']['RESP']),
	# 			array(mock_df['value']['PULSE']),
	# 			array(mock_df['value']['etco2']),
	# 			array(mock_df['value']['SpO2']),
	# 			array(mock_df['value']['CVP']),
	# 			array(mock_df['value']['AWRR']),
	# 			array(mock_df['value']['NBP_Mean']),
	# 			array(mock_df['value']['NBP_Dias']),
	# 			array(mock_df['value']['NBP_Sys'])
	# 		))
	# Define the function to preprocess and send the data to Kafka
	def preprocess_and_send_to_kafka(batch_df, batch_id):
		"""The function is fed into foreachBatch of a writestream.
		It preprocesses the data and sends it to Kafka.
		Thus it should be modified for necessary preprocessing"""

		# Preprocess the data
		# Here it does nothing.
		preprocessed_df = batch_df #.select(lower(col("value")).alias("value_lower"))

		# package the "value", "timestamp" and "ID" into one array 
		# and send it to Kafka
		# preprocessed_df = preprocessed_df.select(
		#     to_json(struct(
		#         lit(124),
		#         lit(256),
		#         lit(358)
		#     )).alias("structs")
		# )

		# This is the mock data from one patient

		# Convert data in mock_df to a column in preprocessed_Df, which is a column called struct 
		# the struct contains 10 columns, one for each column of mock_df.
		# all the values of each column is stored as an array

		
		# preprocessed_df = preprocessed_df.withColumn("structs",
		# 	to_json(struct(
		# 		array(lit(124), lit(124)).alias("HR"),
		# 		array(lit(124), lit(124)).alias("RESP"),
		# 		array(lit(124), lit(124)).alias("PULSE"),
		# 		array(lit(124), lit(124)).alias("etco2"),
		# 		array(lit(124), lit(124)).alias("SpO2"),
		# 		array(lit(124), lit(124)).alias("CVP"),
		# 		array(lit(124), lit(124)).alias("AWRR"),
		# 		array(lit(124), lit(124)).alias("NBP_Mean"),
		# 		array(lit(124), lit(124)).alias("NBP_Dias"),
		# 		array(lit(124), lit(124)).alias("NBP_Sys")
		# 	)
		# 	))
		# preprocessed_df = preprocessed_df.withColumn(
		# 	'structs',
		# 	to_json(struct(
		# 		array(mock_df['0']).alias("HR"),
		# 		array(mock_df['1']).alias("RESP"),
		# 		array(mock_df['2']).alias("PULSE"),
		# 		array(mock_df['3']).alias("etco2"),
		# 		array(mock_df['4']).alias("SpO2"),
		# 		array(mock_df['5']).alias("CVP"),
		# 		array(mock_df['6']).alias("AWRR"),
		# 		array(mock_df['7']).alias("NBP_Mean"),
		# 		array(mock_df['8']).alias("NBP_Dias"),
		# 		array(mock_df['9']).alias("NBP_Sys")
		# 	))
		# )
		# hard coding some random values
		preprocessed_df = preprocessed_df.withColumn(
			'structs',
			to_json(struct(
				array([lit(95.0) for _ in range(120)]).alias("HR"),
				array([lit(95.0) for _ in range(120)]).alias("RESP"),
				array([lit(95.0) for _ in range(120)]).alias("PULSE"),
				array([lit(95.0) for _ in range(120)]).alias("etco2"),
				array([lit(95.0) for _ in range(120)]).alias("SpO2"),
				array([lit(95.0) for _ in range(120)]).alias("CVP"),
				array([lit(95.0) for _ in range(120)]).alias("AWRR"),
				array([lit(95.0) for _ in range(120)]).alias("NBP_Mean"),
				array([lit(95.0) for _ in range(120)]).alias("NBP_Dias"),
				array([lit(95.0) for _ in range(120)]).alias("NBP_Sys")
			)))
		#add a column "patient_ID" to the dataframe with a fixed value of 1
		preprocessed_df = preprocessed_df.withColumn("patient_ID", lit(2659))
		
		# Send the preprocessed data to Kafka
		preprocessed_df.selectExpr(
        "CAST(patient_ID AS STRING) AS key",
        "structs AS value" # oh my, this took me so long. you have to specify "structs" as the value of the key "value". otherwise it will send the "value" column 
    ).write.format("kafka")\
			.options(**producer_conf)\
			.option("topic", "call-stream")\
			.option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
			.option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
			.option("key", "patient_ID")\
			.save()
		
		# to do: write to the same topic but different ID for each patient ID.
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
		# base_df = df.selectExpr("CAST(value as STRING) AS a", "timestamp", "1 as ID") # This will throw error!
		base_df = df.selectExpr("CAST(value as STRING)", "timestamp")
		
		# to see what "base_df" is like in the stream,
		# Uncomment base_df.writeStream.outputMode(...)
		# and comment out base_df.writeStream.foreachBatch(...)
		# query = base_df.writeStream.outputMode("append").format("console").trigger(processingTime='10 seconds').start()
		# query.await_termination()

		# Write the preprocessed DataFrame to Kafka in batches.
		kafka_writer: DataStreamWriter = base_df.writeStream.foreachBatch(preprocess_and_send_to_kafka)
		kafka_query: StreamingQuery = kafka_writer.start()
		print("await termination")
		kafka_query.awaitTermination()

	msg_process(consumer_conf['bootstrap.servers'], 
				args.preprocessing_topic)

if __name__ == "__main__":
	main()