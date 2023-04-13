import argparse
import wfdb
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import numpy as np
from utils import evaluate, load_dataset
import torch
from torch import nn
from models import MyCNN
from utils import get_global_config, compute_batch_accuracy, acked, get_producer_config, get_consumer_config, build_spark_session, get_waveform_path, create_batch, get_arr, get_base_time, get_ending_time,get_record,run_model,run_model_dummy
from pyspark.sql.functions import explode, split, from_json, to_json, col, struct, window, avg, collect_list,first
import pyspark.pandas as ps
import mysql.connector
import datetime
import pandas as pd
from pyspark.sql.types import *

cfg = get_global_config()
cnx = mysql.connector.connect(user='root', 
							  password='mauFJcuf5dhRMQrjj',
							  host='172.18.0.8', 
							  database='mydb')

i=0
# Define a function to write each batch of streaming data to MySQL
def write_to_mysql(batch_df, batch_id):

# batch_df looks like this.

# time-series-kafka-demo-deeplearning-1   | +---------+-------------------+-------------------+--------------------+
# time-series-kafka-demo-deeplearning-1   | |patientid|         start_time|           end_time|              value2|
# time-series-kafka-demo-deeplearning-1   | +---------+-------------------+-------------------+--------------------+
# time-series-kafka-demo-deeplearning-1   | |  p000194|2023-04-10 08:08:18|2023-04-10 08:08:23|{"col1":"RESP","c...|
# time-series-kafka-demo-deeplearning-1   | |  p000194|2023-04-10 08:08:16|2023-04-10 08:08:21|{"col1":"RESP","c...|
# time-series-kafka-demo-deeplearning-1   | |  p000194|2023-04-10 08:08:20|2023-04-10 08:08:25|{"col1":"RESP","c...|
# time-series-kafka-demo-deeplearning-1   | +---------+-------------------+-------------------+--------------------+
	# preprocessed_df = batch_df.groupby("key","start_time","start","end")\
	# 	.agg(to_json(collect_list("average")).alias("value")) \
	# 	.selectExpr(
	# 	'concat(key, "_", channel) as key',
	# 	'value'
	# )

# If we were to perform batch_df.collect() and print it, we will receive this.
# [Row(patientid='p000194', start_time=datetime.datetime(2023, 4, 10, 8, 15, 44), end_time=datetime.datetime(2023, 4, 10, 8, 15, 49), value2='{"channel":"HR","lst":[[77.19999694824219],[9.199999809265137],[12.699999809265137],[12.699999809265137],[100.0],[100.0],[98.5999984741211],[77.19999694824219],[77.19999694824219],[100.0],[78.5999984741211],[78.5999984741211],[75.19999694824219],[14.300000190734863],[15.0],[77.69999694824219],[100.0],[78.5999984741211],[76.9000015258789],[75.0999984741211],[99.80000305175781],[15.0],[76.4000015258789],[98.5999984741211],[9.199999809265137],[75.19999694824219],[75.0999984741211],[12.699999809265137],[15.0],[99.80000305175781],[76.4000015258789],[76.4000015258789]]}'), Row(patientid='p000194', start_time=datetime.datetime(2023, 4, 10, 8, 15, 42), end_time=datetime.datetime(2023, 4, 10, 8, 15, 47), value2='{"channel":"PULSE","lst":[[76.4000015258789],[75.19999694824219],[76.4000015258789],[98.5999984741211],[77.19999694824219],[9.199999809265137],[12.699999809265137],[12.699999809265137],[100.0],[100.0],[98.5999984741211],[77.19999694824219],[77.19999694824219],[100.0],[78.5999984741211],[78.5999984741211],[75.19999694824219],[14.300000190734863],[15.0],[77.69999694824219],[100.0],[78.5999984741211],[76.9000015258789],[75.0999984741211],[99.80000305175781],[15.0],[76.4000015258789],[98.5999984741211],[9.199999809265137],[75.19999694824219],[75.0999984741211],[12.699999809265137],[15.0],[99.80000305175781],[76.4000015258789],[76.4000015258789]]}'), Row(patientid='p000194', start_time=datetime.datetime(2023, 4, 10, 8, 15, 40), end_time=datetime.datetime(2023, 4, 10, 8, 15, 45), value2='{"channel":"PULSE","lst":[[76.4000015258789],[75.19999694824219],[76.4000015258789],[98.5999984741211],[77.19999694824219],[9.199999809265137],[12.699999809265137],[12.699999809265137],[100.0],[100.0],[98.5999984741211],[77.19999694824219],[77.19999694824219],[100.0],[78.5999984741211],[78.5999984741211],[75.19999694824219],[14.300000190734863],[15.0],[77.69999694824219],[100.0],[78.5999984741211],[76.9000015258789],[75.0999984741211],[99.80000305175781],[15.0],[76.4000015258789],[98.5999984741211],[9.199999809265137],[75.19999694824219],[75.0999984741211],[12.699999809265137],[15.0],[99.80000305175781],[76.4000015258789],[76.4000015258789]]}')]
	collected = batch_df.collect()

	# Call ML model

	# Send results to MySQL
	print(collected)

	# cursor = cnx.cursor()

	# # Convert the batch DataFrame to a list of tuples
	# data = [tuple(row) for row in batch_df.collect()]

	# # Construct the SQL query to insert the data into MySQL
	# query = "INSERT INTO mytable (col1, col2) VALUES (%s, %s)"

	# # Insert the data into MySQL using a prepared statement
	# cursor.executemany(query, data)
	# cnx.commit()


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



	def msg_process(server, topic):
		"""Create a streaming dataframe that takes in values of messages received, 
		together with the current timestamp.
		Then, print them out.
		Then, process the message in batch
		Reference link: https://medium.com/@aman.parmar17/handling-real-time-kafka-data-streams-using-pyspark-8b6616a3a084"""

		base_df = (spark.
				readStream
				.format("kafka")
				.option("kafka.bootstrap.servers", server)
				.option("subscribe", topic)
				.option("startingOffsets", "latest")
				.load()
			)
		print(base_df)
		# Select the value and timestamp (the message is received)
		# base_df = df.selectExpr("CAST(key as STRING) as key", "CAST(value as STRING)", "timestamp")

		# cast key as string
		base_df = base_df.withColumn("key",col("key").cast("string"))
		base_df = base_df.withColumn("patientid",split(col("key"), '_').getItem(0)) #get signal name
		base_df = base_df.withColumn("channel",split(col("key"), '_').getItem(1)) #get signal name
		base_df = base_df.withColumn("parsed",from_json(col("value").cast("string"), "array<double>"))


		# Split key by "_" and create a new column called "signal"




		# Parse the JSON strings back into a struct column
		# base_df = base_df.select(from_json(col("value"), "struct<average:array<double>>").alias("parsed"))
# to_json(collect_list(struct("channel","average"))

		# .agg(to_json(collect_list("parsed")).alias("value2"))  \
		# struct_schema = StructType([
		# 		StructField('channel', StringType(), True),
		# 		StructField('parsed', ArrayType(), True),
		# 	])
		base_df = base_df.withWatermark("timestamp", "3 seconds") \
		.groupBy(
			base_df.patientid,
			window("timestamp", "5 seconds", '2 seconds')) \
		.agg(to_json(struct(first("channel").alias("channel"),collect_list("parsed").alias("lst"))).alias("value2")) \
			.selectExpr(
			"patientid",
			# "channel",
			"window.start as start_time",
			"window.end as end_time",
			"value2"
		)
		# base_df = base_df.select("key",
		# 	split(base_df['key'],'_').getItem(0).alias("patient_id"),
		# 	split(base_df['key'],'_').getItem(1).alias("signal_name"),
		# 	"start_time",
		# 	"end_time",
		# 	"value2"
		# )

		# print(base_df)
		# base_df looks like this

# 		time-series-kafka-demo-deeplearning-1   | +-------------+-------------------+-------------------+--------------------+
# time-series-kafka-demo-deeplearning-1   | |          key|         start_time|           end_time|              value2|
# time-series-kafka-demo-deeplearning-1   | +-------------+-------------------+-------------------+--------------------+
# time-series-kafka-demo-deeplearning-1   | |   p000194_HR|2023-04-10 07:33:04|2023-04-10 07:33:09|[[77.400001525878...|
# time-series-kafka-demo-deeplearning-1   | | p000194_RESP|2023-04-10 07:33:04|2023-04-10 07:33:09|[[15.0],[14.10000...|
# time-series-kafka-demo-deeplearning-1   | |p000194_PULSE|2023-04-10 07:33:02|2023-04-10 07:33:07|[[76.400001525878...|
# time-series-kafka-demo-deeplearning-1   | | p000194_SpO2|2023-04-10 07:33:02|2023-04-10 07:33:07|[[100.0],[100.0],...|
# time-series-kafka-demo-deeplearning-1   | |p000194_PULSE|2023-04-10 07:33:04|2023-04-10 07:33:09|[[76.400001525878...|
# time-series-kafka-demo-deeplearning-1   | | p000194_RESP|2023-04-10 07:33:02|2023-04-10 07:33:07|[[15.0],[14.10000...|
# time-series-kafka-demo-deeplearning-1   | |   p000194_HR|2023-04-10 07:33:02|2023-04-10 07:33:07|[[77.400001525878...|
# time-series-kafka-demo-deeplearning-1   | | p000194_SpO2|2023-04-10 07:33:04|2023-04-10 07:33:09|[[100.0],[100.0],...|
# time-series-kafka-demo-deeplearning-1   | +-------------+-------------------+-------------------+--------------------+
# time-series-kafka-demo-deeplearning-1   | 
		# 1/0
		
		# The model needs to be called to store the data
		model = torch.load(cfg['MODELPATH'])
		device = torch.device("cuda" if cfg['USE_CUDA'] and torch.cuda.is_available() else "cpu")

		# Once we manage to set up the actual data, 
		# we will pull data from Kafka stream instead of having to retrieve 
		# data from local.
		data_df = pd.read_csv((cfg['EXPLOREPATH']+'/X.TESTINPUT'))

		# We will also be retrieving the age from the patient_id
		age = 60

		y_pred, y_prob = run_model(model, device, data_df, age)

		## to see what "base_df" is like in the stream,
		## Uncomment base_df.writeStream.outputMode(...)
		## and comment out base_df.writeStream.foreachBatch(...)
		# query = base_df.writeStream.outputMode("append").format("console").trigger(processingTime='10 seconds').start()
		# query.awaitTermination()
		
		# Write the streaming data to MySQL using foreachBatch
		query = base_df.writeStream.foreachBatch(write_to_mysql).trigger(processingTime='10 seconds').start()
		
		# for i in range(10):
		# 	print("Model has successfully run!")
		# 	print(y_pred)
		# 	print(y_prob)
		
		# # Store the results in y_pred and y_prob
		# # run_model_dummy()

		# # Create the first cursor for executing queries on the 'mytable' table
		# cursor1 = cnx.cursor()
		# query1 = 'SELECT * FROM mytable'
		# cursor1.execute(query1)
		# rows1 = cursor1.fetchall()
		# print('Rows from mytable:')
		# for idx, row in enumerate(rows1):
		# 	# Too verbose
		# 	if idx %50==0:
		# 		print(row)
		# 	idx+=1
		
		query.awaitTermination()

		return query
		

	query = msg_process(consumer_conf['bootstrap.servers'], args.model_call_topic)

	raise Exception

if __name__ == "__main__":
	#print("ML module!")
	main()