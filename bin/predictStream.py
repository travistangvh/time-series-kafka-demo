import argparse
import wfdb
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import numpy as np
from utils import evaluate, load_dataset
import torch
from torch import nn
from models import MyCNN
from utils import get_global_config, compute_batch_accuracy, acked, get_producer_config, get_consumer_config, build_spark_session, get_waveform_path, create_batch, get_arr, get_base_time, get_ending_time,get_record,run_model,run_model_dummy
from pyspark.sql.functions import explode, split, from_json, to_json, col, struct
import pyspark.pandas as ps
import mysql.connector
import datetime
import pandas as pd
from pyspark.sql.functions import explode, split, from_json, to_json, col, struct, window, avg, collect_list,first
import logging

cfg = get_global_config()
cnx = mysql.connector.connect(user='root', 
							  password='mauFJcuf5dhRMQrjj',
							  host='172.18.0.8', 
							  database='mydb')

# Define a function to write each batch of streaming data to MySQL
def write_to_mysql(batch_df, batch_id):
	# configure logger
	logger = logging.getLogger(__name__)
	logger.setLevel(logging.INFO)
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	ch = logging.StreamHandler()
	ch.setFormatter(formatter)
	logger.addHandler(ch)
	
	# Send results to MySQL
	cursor = cnx.cursor()

	# To do: Call ML model on streaming data

	# To do: Collect results from ML Model on streaming data

	# To do: Send results to MySQL using the following code:

	# Collect the data from the batch DataFrame
	collected = batch_df.collect()

	# Convert the batch DataFrame to a list of tuples
	# data = [tuple(row) for row in collected]

	# Construct the SQL query to insert the data into MySQL
	data = []

	# Truncate data that is too long
	for row in collected:
		truncated_row = []
		for value in row:
			if isinstance(value, str):
				truncated_row.append(value[:100])
			else:
				truncated_row.append(value)
		data.append(tuple(truncated_row))

	# col1: patientid, col2: starttime, col3: endtime, col4: lst
	query = "INSERT INTO mytable (col1, col2, col3, col4) VALUES (%s, %s, %s, %s)"

	# Insert the data into MySQL using a prepared statement
	cursor.executemany(query, data)
	cnx.commit()

	# Create the first cursor for executing queries on the 'mytable' table
	cursor1 = cnx.cursor()
	query1 = "SELECT col1, col2, col3, col4 FROM mytable WHERE col1!='hi' ORDER BY col2 desc LIMIT 1"
	cursor1.execute(query1)
	rows1 = cursor1.fetchall()
	logger.info('Rows from mytable:')
	logger.info(rows1)

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
		
		# Perform some preprocessing
		base_df = base_df.withColumn("key",col("key").cast("string"))
		base_df = base_df.withColumn("patientid",split(col("key"), '_').getItem(0)) #get patient ID
		base_df = base_df.withColumn("channel",split(col("key"), '_').getItem(1)) #get signal name
		base_df = base_df.withColumn("parsed",from_json(col("value").cast("string"), "array<double>"))

		# In the test case, each window has a duration of 5 seconds. The interval between each window is 2 seconds interval
		# In the actual case, each window has a duration of 10 minutes. The interval between each window is 1 minute interval.
		# The result from the following is suitable for machine learning inference. 
		# This is because each row of the dataframe will contain all the necessary signals for one machine learning input.
		base_df = base_df.withWatermark("timestamp", "3 seconds") \
		.groupBy(
			base_df.patientid,
			window("timestamp", "5 seconds", '2 seconds')) \
		.agg(to_json(struct(first("channel").alias("channel"),collect_list("parsed").alias("lst"))).alias("value2")) \
			.selectExpr(
			"patientid",
			"window.start as start_time",
			"window.end as end_time",
			"value2"
		)

		# Temporarily, to demonstrate that our model can indeed be run, 
		# We will use hard-coded waveform data in the form of a CSV.
		# This is because we have yet to connect the result from hte processStream to the machine learning model.
		# Once we manage to set up the actual data, 
		# we will pull data from Kafka stream instead of having to retrieve data from local.

		# Retrieve the model
		model = torch.load(cfg['MODELPATH'])
		device = torch.device("cuda" if cfg['USE_CUDA'] and torch.cuda.is_available() else "cpu")
		data_df = pd.read_csv((cfg['EXPLOREPATH']+'/X.TESTINPUT'))

		# We will also be retrieving the age from the patient_id. 
		age = 60

		y_pred, y_prob = run_model(model, device, data_df, age)

		print("Model has successfully run!")
		print(y_pred)
		print(y_prob)
		
		# Write the streaming data to MySQL using foreachBatch
		query = base_df.writeStream.foreachBatch(write_to_mysql).trigger(processingTime='10 seconds').start()
		
		# Uncomment the following line in order to print the streaming data to console
		# query = base_df.writeStream.outputMode("append").format("console").option("truncate", "false").trigger(processingTime='10 seconds').start()
			
		query.awaitTermination()

		return query

	query = msg_process(consumer_conf['bootstrap.servers'], args.model_call_topic)


if __name__ == "__main__":
	#print("ML module!")
	main()