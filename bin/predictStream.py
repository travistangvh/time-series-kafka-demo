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
from collections import defaultdict

"""Defining global variables"""
cfg = get_global_config()

# Define MySQL connection
cnx = mysql.connector.connect(user='root', 
							  password='mauFJcuf5dhRMQrjj',
							  host='172.18.0.8', 
							  database='mydb')

# Get a patients_age table from MySQL
cursor = cnx.cursor()
query = "SELECT SUBJECT_ID, CAST(DATEDIFF(CURRENT_DATE,DOB) AS DOUBLE)/365 age FROM patients_age"
cursor.execute(query)
rows = cursor.fetchall()
patients_age_df = pd.DataFrame(rows, columns=['patientid', 'age'])

# Loading the model
model = torch.load(cfg['MODELPATH'])
model.eval()
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# Creating spark
spark = build_spark_session()

"""Helper functions"""
# Define a function to write each batch of streaming data to MySQL
def write_to_mysql(batch_df, batch_id):
	
	# Send results to MySQL
	cursor = cnx.cursor()

	# Collect data in the correct format.
	batch_df = batch_df.withColumn("v_parsed",from_json(col("value2").cast("string"), "struct<channel:array<int>,lst:array<array<double>>>"))
	
	# Select only the relevant columns from batch_df 
	# To do: select only relevant columns so collect is faster.
	batch_df = batch_df.select("patientid", "start_time", "end_time", "v_parsed")

	collected = batch_df.collect()

	# Construct the SQL query to insert the data into MySQL
	data = []

	# Truncate data that is too long
	for row in collected:
		if all(field is None for field in row):
			# Ignore empty row
			logger.info("Row is empty")
		else:
			logger.info("Row is not empty")
		
			pid = int(row.patientid[1:])
			start_time = row.start_time
			end_time = row.end_time
			v_parsed = row.v_parsed
			channel = v_parsed.channel # channel is a list, e.g. ['HR',  	  'PULSE', 		'RESP', 	'HR', 			'RESP']. It serves as a way to identify the signal
			lst = v_parsed.lst # lst is now a list of list 		 [[0.1, 0.2], [0.2,0.4],    [0.4,0.6],  [0.5, 0.3]. 	[0.1,0.6]]
								# together with channel, we can read that the first signal of HR is 0.1, 0.2, followed by 0.5,0.3 , the first signal of pulse is 0.2, 0.4, etc.
								# There are as many items in lst as there are signals in channel.

			logger.info(f"row start time {start_time} endtime {end_time} lst_length {len(lst)}")

			# Flatten list of list
			logger.info(v_parsed)
			logger.info(channel)
			channel = [item for item in channel]
			# lst2 = [item for sublist in lst for item in sublist]

			# Create a dictionary to store the signal name and its values
			d = defaultdict(list)
	
			for signal_index in channel:
				d[signal_index] = []

			for signal_index, val in zip(channel, lst):
				d[signal_index].extend(val)

			# Construct a numpy array using all the values in the dictionary
			x_arr = np.empty((1, 10, 120))

			# Create a reverse lookup table for the signal names
			# lookup = {v: k for k, v in enumerate(cfg['CHANNEL_NAMES'])}

			# Iterate through all signals
			for signal_name in cfg['CHANNEL_NAMES']:
				# If this is the first time a signal is encountered.
				if cfg['CHANNEL_NAMES'].index(signal_name) in d:
					# Create a numpy array 
					signal = np.array(d[signal_index])
					
					# If signal has less than 120 items, we need to wait until it has enough items.
					if signal.shape[0] < 120:
						logger.info(f"Waiting until there are 120 signals. Now it only has {signal.shape[0]}")
						logger.info(f"Signal: {signal}")
						return
					
					# If signal has at least 120 items, we truncate it to 120 items.
					if signal.shape[0] >= 120:
						logger.info(f"You need to make sure that there are 120 signals!! Now it has {signal.shape[0]}")
						signal = signal[:120]

				# If signal is not present at all, we create a numpy array of zeros.		
				else:
					signal = np.zeros(120)

				# Add the numpy array to the x_arr array. 
				# x_arr is the input to the ML model 
				# It has the shape (1, 10, 120).
				# The first dimension is the batch size. We only have one.
				# The second dimension is the number of signals. We have 10 signals. 
				# The third dimension is the number of items in each signal. We have 120 items in each signal.
				x_arr[0, signal_index, :] = signal
				
			# Make sure that the shape of x_arr is correct
			assert x_arr.shape == (1, 10, 120)

			# Get the age of the patient
			try:
				patient_age = patients_age_df.query("patientid == @pid")["age"].values[0]
				a_arr = get_arr(x_arr, patient_age)
			except:			
				# If not present, we'll use a default age
				age = 65.0
				a_arr = get_arr(x_arr, age)

			# Evaluate x_arr
			with torch.no_grad():
				x_arr = torch.from_numpy(x_arr).type(torch.FloatTensor).to(device).float()
				a_arr = torch.from_numpy(a_arr).type(torch.FloatTensor).unsqueeze(0).to(device).float()
				output = model(x_arr, a_arr)
				x_arr=x_arr.to(device)
				a_arr=a_arr.to(device)
				y = torch.sigmoid(output).detach().to('cpu')
				y_pred = y.round().long().numpy().tolist()[0]
				y_prob = y.numpy().tolist()[0]
			
			# Store the data
			data = [tuple([str(pid), str(start_time), str(y_prob), str(y_prob)])]

			# col1: patientid, col2: starttime, col3: endtime, col4: lst
			# query = "INSERT INTO predictions (SUBJECT_ID, PRED_TIME, RISK_SCORE) VALUES (%s, %s, %s)"
			query = "INSERT INTO mytable (COL1, COL2, COL3, COL4) VALUES (%s, %s, %s, %s)"

			# Insert the data into MySQL using a prepared statement
			cursor.executemany(query, data)
			cnx.commit()

			# Create the first cursor for executing queries on the 'mytable' table
			# To do: store data in the correct table.
			cursor1 = cnx.cursor()
			query1 = "SELECT col1,col2,col3,col4 FROM mytable WHERE col1!= 'hi' ORDER BY COL2 DESC LIMIT 5"
			# query1 = "SELECT PREDICTION_ID, SUBJECT_ID, PRED_TIME, RISK_SCORE FROM predictions LIMIT 1"
			cursor1.execute(query1)
			rows1 = cursor1.fetchall()
			logger.info(f'Rows from predictions:{rows1}')

		# For debugging: We can stop the streaming query after a certain number of batches.
		logger.info(f'batch_id: {batch_id}')
		# if batch_id >= 10:
		# 	print("Stopping the streaming query...")
		# 	raise Exception

def main():
	"""Get arguments from command line"""
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument('--model-call-topic', 
						type=str,
						help='Name of the Kafka topic to receive machine learning call.')

	parser.add_argument('--model-response-topic', 
						type=str,
						help='Name of the Kafka topic to send the response to the call.')
	
	parser.add_argument('--speed', type=float, default=1, required=False,
						help='Speed up time series by a given multiplicative factor.')
	
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
		base_df = base_df.withColumn("channel",split(col("key"), '_').getItem(1).cast("int")) #get signal name
		base_df = base_df.withColumn("parsed",from_json(col("value").cast("string"), "array<double>"))

		# logger.info(f"base_df {base_df}")

		# In the actual case, each window has a duration of 600 seconds. The interval between each window is 60 seconds.
		# We take a window of 600 seconds (10 minutes) and slide it every 60 seconds.
		# Within 10 minutes, we would have accumulated 120 data points for each signal.
		base_df = base_df.withWatermark("timestamp", f'{int(10/args.speed)} seconds') \
		.groupBy(
			base_df.patientid,
			window("timestamp", f'{int(600/args.speed)} seconds', f'{int(60/args.speed)} seconds')
			) \
		.agg(to_json(struct(collect_list("channel").alias("channel"),collect_list("parsed").alias("lst"))).alias("value2")) \
			.selectExpr(
			"patientid",
			"window.start as start_time",
			"window.end as end_time",
			"value2"
		)
		
		# Write the streaming data to MySQL using foreachBatch.
		# This sends the data to the model every 60 seconds.
		query = base_df.writeStream.foreachBatch(write_to_mysql).trigger(processingTime=f'{int(60/args.speed)} seconds').start()
		
		# Uncomment the following line in order to print the streaming data to console
		# query = base_df.writeStream.outputMode("append").format("console").option("truncate", "false").trigger(processingTime='10 seconds').start()
			
		query.awaitTermination()

		return query

	query = msg_process(consumer_conf['bootstrap.servers'], args.model_call_topic)

if __name__ == "__main__":
	main()