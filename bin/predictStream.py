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

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Loading the model
model = torch.load(cfg['MODELPATH'])

model.eval()

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
	# Convert a struct into a numpy array
	# batch_df = batch_df.select(col("value").cast("string"))

	# To do: Collect results from ML Model on streaming data

	# To do: Send results to MySQL using the following code:
	# encoding: agg(to_json(collect_list("average")).alias("value")) \
	# decoding: withColumn("parsed",from_json(col("value").cast("string"), "array<double>"))
	# encoding: .agg(to_json(struct(first("channel").alias("channel"),collect_list("parsed").alias("lst"))).alias("value2")) \
	# decoding:
	batch_df = batch_df.withColumn("v_parsed",from_json(col("value2").cast("string"), "struct<channel:array<string>,lst:array<array<double>>>"))
	collected = batch_df.collect()

	# logging.info(collected)
	# patient_id = int(collected[0][0][1:])
	logger.info(f"Collected: {collected}")
	# logger.info(np.array(collected[0]))
	# logging.info("shape is {}".format(np.array(collected).shape))
	# Collect the data from the batch DataFrame
	# Convert the batch DataFrame to a list of tuples
	# data = [tuple(row) for row in collected]

	# Construct the SQL query to insert the data into MySQL
	data = []

	# Truncate data that is too long
	for row in collected:
		truncated_row = []
		
		# for value in row:
		# 	if isinstance(value, str):
		# 		truncated_row.append(value[:100])
		# 	else:
		# 		truncated_row.append(value)
		# This is a row:
		# Row(patientid='p000194', start_time=datetime.datetime(2023, 4, 15, 7, 6, 54), end_time=datetime.datetime(2023, 4, 15, 7, 7, 54), value2='{"channel":"NBP Dias","lst":[[0.0],[0.0],[91.26037735849057],[90.648393194707],[0.0],[90.96558317399618],[18.399617590822178],[98.82608695652173],[0.3349716544489329],[98.81261950286807],[90.37093690248565],[0.0],[0.0],[98.8811320754717],[0.0],[0.0],[90.78301886792453],[0.0],[0.0],[0.3319311763656755],[0.0],[20.20943396226415],[91.13421550094517],[0.0],[0.0],[0.334905670217748],[19.716446124763706],[98.99249530956848],[90.34962406015038],[0.0],[0.0],[90.8609022556391],[20.18609022556391],[0.0],[0.33533835567926107],[0.0],[99.16635160680529],[20.113421550094518],[0.0],[0.0],[91.02626641651032],[90.58161350844277],[0.0],[0.0],[0.0],[0.0],[99.09774436090225],[0.3406427316156362],[0.0],[0.0],[90.13421550094517],[0.33470920309787844],[90.65973534971644],[20.118198874296436],[0.3459047710895538],[0.0],[90.0326923076923],[0.0],[90.62095238095237],[0.0],[99.19807692307693],[99.2088122605364],[19.13269230769231],[0.0],[99.24665391969407],[0.3560229531323705],[0.0],[0.0],[0.0],[0.0],[90.41108986615679],[0.0],[90.06130268199233],[90.06666666666666],[89.68461538461538],[90.54980842911877],[0.0],[0.0],[89.97705544933078],[0.35019157981050425],[0.0],[19.78776290630975],[99.17142857142858],[0.0],[0.0],[0.0],[0.35865385457873344],[19.942857142857143],[0.0],[19.85632183908046]]}', 
		# v_parsed=Row(channel='NBP Dias', lst=[[0.0], [0.0], [91.26037735849057], [90.648393194707], [0.0], [90.96558317399618], [18.399617590822178], [98.82608695652173], [0.3349716544489329], [98.81261950286807], [90.37093690248565], [0.0], [0.0], [98.8811320754717], [0.0], [0.0], [90.78301886792453], [0.0], [0.0], [0.3319311763656755], [0.0], [20.20943396226415], [91.13421550094517], [0.0], [0.0], [0.334905670217748], [19.716446124763706], [98.99249530956848], [90.34962406015038], [0.0], [0.0], [90.8609022556391], [20.18609022556391], [0.0], [0.33533835567926107], [0.0], [99.16635160680529], [20.113421550094518], [0.0], [0.0], [91.02626641651032], [90.58161350844277], [0.0], [0.0], [0.0], [0.0], [99.09774436090225], [0.3406427316156362], [0.0], [0.0], [90.13421550094517], [0.33470920309787844], [90.65973534971644], [20.118198874296436], [0.3459047710895538], [0.0], [90.0326923076923], [0.0], [90.62095238095237], [0.0], [99.19807692307693], [99.2088122605364], [19.13269230769231], [0.0], [99.24665391969407], [0.3560229531323705], [0.0], [0.0], [0.0], [0.0], [90.41108986615679], [0.0], [90.06130268199233], [90.06666666666666], [89.68461538461538], [90.54980842911877], [0.0], [0.0], [89.97705544933078], [0.35019157981050425], [0.0], [19.78776290630975], [99.17142857142858], [0.0], [0.0], [0.0], [0.35865385457873344], [19.942857142857143], [0.0], [19.85632183908046]]))
		
		if all(field is None for field in row):
			logger.info("Row is empty")
		else:
			logger.info("Row is not empty")
		
			pid = int(row.patientid[1:])
			start_time = row.start_time
			end_time = row.end_time
			# value = row.value[0]
			v_parsed = row.v_parsed

			# channel='NBP Dias', lst=[[0.0], [0.0], [91.26037735849057], [90.648393194707], [0.0], [90.96558317399618], [18.399617590822178], [98.82608695652173], [0.3349716544489329], [98.81261950286807], [90.37093690248565], [0.0], [0.0], [98.8811320754717], [0.0], [0.0], [90.78301886792453], [0.0], [0.0], [0.3319311763656755], [0.0], [20.20943396226415], [91.13421550094517], [0.0], [0.0], [0.334905670217748], [19.716446124763706], [98.99249530956848], [90.34962406015038], [0.0], [0.0], [90.8609022556391], [20.18609022556391], [0.0], [0.33533835567926107], [0.0], [99.16635160680529], [20.113421550094518], [0.0], [0.0], [91.02626641651032], [90.58161350844277], [0.0], [0.0], [0.0], [0.0], [99.09774436090225], [0.3406427316156362], [0.0], [0.0], [90.13421550094517], [0.33470920309787844], [90.65973534971644], [20.118198874296436], [0.3459047710895538], [0.0], [90.0326923076923], [0.0], [90.62095238095237], [0.0], [99.19807692307693], [99.2088122605364], [19.13269230769231], [0.0], [99.24665391969407], [0.3560229531323705], [0.0], [0.0], [0.0], [0.0], [90.41108986615679], [0.0], [90.06130268199233], [90.06666666666666], [89.68461538461538], [90.54980842911877], [0.0], [0.0], [89.97705544933078], [0.35019157981050425], [0.0], [19.78776290630975], [99.17142857142858], [0.0], [0.0], [0.0], [0.35865385457873344], [19.942857142857143], [0.0], [19.85632183908046]]

			channel = v_parsed.channel
			lst = v_parsed.lst

			# logger.info(f"Patientid: {pid}")
			# logger.info(f"Channel: {channel}")
			# logger.info(f"Starttime: {start_time}")
			# logger.info(f"Endtime: {end_time}")
			# logger.info(f"lst: {lst}")
			# Convert lst from a list of a list to a list
			channel = [item for item in channel]
			lst2 = [item for sublist in lst for item in sublist]
			logger.info(f"channel: {channel}")
			logger.info(f"lst2: {lst2}")

			from collections import defaultdict
			d = defaultdict(list)
	
			for signal_name, val in zip(channel, lst2):
				d[signal_name].append(val)

			logger.info(f"dict: {d}")
			# Construct a numpy array using all the values in the dictionary
			x_arr = np.empty((1, 10, 120))

			# Repeat signal until it has 120 elements
			for signal_name in cfg['CHANNEL_NAMES']:
				# Create a numpy array with 120 items
				if signal_name in d:
					signal = np.array(d[signal_name])
					# fill signal with zeros if it has less than 120 items
					if signal.shape[0] < 120:
						signal = np.pad(signal, (0, 120 - signal.shape[0]), 'mean')
						logger.info("You need to make sure that there are 120 signals!!")
				else:
					signal = np.zeros(120)
				# Add the numpy array to the x_arr array
				x_arr[0, cfg['CHANNEL_NAMES'].index(signal_name), :] = signal
			logger.info(f"x_arr: {x_arr}")

			assert x_arr.shape == (1, 10, 120)

			age = 65.0
			# # generate an age array of the same size as x_arr
			try:
				patient_age = patients_age_df.query("patientid == @pid")["age"].values[0]
				logging.info("patient_id is {}".format(pid))
				logging.info("patient_age is {}".format(patient_age))
				a_arr = get_arr(x_arr, patient_age)
			except:			
				a_arr = get_arr(x_arr, age)

			with torch.no_grad():
				x_arr = torch.from_numpy(x_arr).type(torch.FloatTensor).to(device).float()
				a_arr = torch.from_numpy(a_arr).type(torch.FloatTensor).unsqueeze(0).to(device).float()
				output = model(x_arr, a_arr)
				x_arr=x_arr.to(device)
				a_arr=a_arr.to(device)
				y = torch.sigmoid(output).detach().to('cpu')
				y_pred = y.round().long().numpy().tolist()
				y_prob = y.numpy().tolist()
			logger.info(f"y_pred: {y_pred}")
			logger.info(f"y_prob: {y_prob}")
			data = [tuple([str(pid), str(start_time), str(y_prob), str(y_prob)])]

				# create a numpy array with (10, 120)
				# 1 = number of patients
				# 10 = channel


			# for patient_id, start_time, end_time, value2, vparsed in row:
			# 	# check if row is empty
			# 	if not value2:
			# 		continue

			# 	pid = int(patient_id[1:])
			# 	logger.info(f"Patientid: {pid}")
			# 	if isinstance(value2, str):
			# 		truncated_row.append(value2[:100])
			# 	else:
			# 		truncated_row.append(value2)
			# data.append(tuple(truncated_row))
		# except:
		# 	logger.info(f"failure on row: {row}")


			# col1: patientid, col2: starttime, col3: endtime, col4: lst
			# query = "INSERT INTO predictions (SUBJECT_ID, PRED_TIME, RISK_SCORE) VALUES (%s, %s, %s)"
			query = "INSERT INTO mytable (COL1, COL2, COL3, COL4) VALUES (%s, %s, %s, %s)"

			# Insert the data into MySQL using a prepared statement
			cursor.executemany(query, data)
			cnx.commit()

			# Create the first cursor for executing queries on the 'mytable' table
			cursor1 = cnx.cursor()
			query1 = "SELECT * FROM mytable WHERE col1!= 'hi' ORDER BY COL3 ASC LIMIT 5"
			# query1 = "SELECT PREDICTION_ID, SUBJECT_ID, PRED_TIME, RISK_SCORE FROM predictions LIMIT 1"
			cursor1.execute(query1)
			rows1 = cursor1.fetchall()
			logger.info(f'Rows from predictions:{rows1}')
			# 2023-04-15 04:35:59,397 INFO [('p000194', '2023-04-15 04:34:54', '2023-04-15 04:34:59', '{"channel":"SpO2","lst":[[100.0],[0.0],[0.0],[0.5],[81.0],[0.0],[0.0],[0.0],[0.0],[0.0],[0.0],[80.4]')]

			# raise Exception

# Define global table
# Read from MySQL into pandas dataframe
cursor = cnx.cursor()
query = "SELECT SUBJECT_ID, CAST(DATEDIFF(CURRENT_DATE,DOB) AS DOUBLE)/365 age FROM patients_age"
cursor.execute(query)
rows = cursor.fetchall()
patients_age_df = pd.DataFrame(rows, columns=['patientid', 'age'])

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
		base_df = base_df.withWatermark("timestamp", "10 seconds") \
		.groupBy(
			base_df.patientid,
			window("timestamp", "60 seconds", '6 seconds')) \
		.agg(to_json(struct(collect_list("channel").alias("channel"),collect_list("parsed").alias("lst"))).alias("value2")) \
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