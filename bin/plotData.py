# Implement this https://stackoverflow.com/questions/63589249/plotly-dash-display-real-time-data-in-smooth-animation

"""Consumes stream for printing all messages to the console."""

import argparse
from confluent_kafka import Consumer, Producer
from pyspark.sql.functions import *
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from utils import acked, get_consumer_config, get_producer_config, build_spark_session, get_global_config
import logging

# Bokeh imports
from bokeh.server.server import Server
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure
from bokeh import layouts 
from bokeh.application import Application
from bokeh.models import ColumnDataSource
from bokeh.io import curdoc
from collections import defaultdict
from threading import Thread
import datetime
cfg = get_global_config()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure
from bokeh import layouts
from bokeh.models import ColumnDataSource
import numpy as np
import pandas as pd
from collections import defaultdict
import time

CHANNEL_NAMES =  ['HR', 'RESP', 'PULSE', 'PVC Rate per Minute', 'SpO2', 'CVP', 'ST V', 'NBP Mean', 'NBP Dias', 'NBP Sys']

df = pd.read_csv(f'{cfg["EXPLOREPATH"]}/X.TESTINPUT')
lst = df.iloc[:,2].to_list()

new_data = defaultdict(dict)
for channel_index, channel_name in enumerate(cfg['CHANNEL_NAMES']):
	new_data[channel_index] = {'x':[],'y':[]}
def modify_doc(doc):
	# Create a new plot
	# plot = figure()
	# source = ColumnDataSource({'x': [], 'y': []})
	# plot.line('x', 'y', source=source)
	sources = defaultdict(ColumnDataSource)
	plots = defaultdict(figure)
	for channel_index, channel_name in enumerate(cfg['CHANNEL_NAMES']):
		sources[channel_index] = ColumnDataSource({'x': [], 'y': []})
		plots[channel_index] = figure(toolbar_location=None, name=f'{channel_name}_fig',
									title=f'{channel_name}',
									x_axis_type = "datetime",    
										tools="pan,wheel_zoom,box_zoom,reset,save",
										min_width=1000, 
										height=150)
		plots[channel_index].scatter('x', 'y', source=sources[channel_index], name=f'{channel_name}_plot')

	def update_data():
		for channel_index, _ in enumerate(cfg['CHANNEL_NAMES']):
			sources[channel_index].stream(new_data[channel_index])
			# new_data[channel_index] = {'x':[],'y':[]} # reset new_data

		# # generate a random number
		# # new_data = {'x': [np.random()], 'y': [np.random()]}
		# for channel_idx, channel_name in enumerate(CHANNEL_NAMES):
		# 	new_data = {'x': [i], 'y': [df.iloc[i,channel_idx]]}
		# 	print(new_data)
		# 	sources[channel_name].stream(new_data)

	# Add a periodic callback to update the plot every second
	doc.add_periodic_callback(update_data, 1000)

	# Add the plot to the document
	# doc.add_root(column(plot))
	for channel_index, _ in enumerate(cfg['CHANNEL_NAMES']):
		doc.add_root(layouts.column(plots[channel_index]))

# Create a new Bokeh server application
app = Application(FunctionHandler(modify_doc))
server = Server({'/': app}, port=5068) # it is crucial to set port=5068, otherwise it will not work
# server.io_loop.start()

# Define a function to start the I/O loop in a separate thread
def start_server():
    server.io_loop.start()

# Start the server in a separate thread
server_thread = Thread(target=start_server)
server_thread.start()

logger.info("unblocked!")

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


	# Define the function to preprocess and send the data to Kafka
	def plot_data(batch_df, batch_id):
		"""The function is fed into foreachBatch of a writestream.
		It preprocesses the data and sends it to Kafka.
		Thus it should be modified for necessary preprocessing"""

		# Create a new key in the format of "patientid_channelname". 
		# An example of the new key is "p000194_SpO2"
		# The "value" here will be an array containing one value of SpO2 of the patient.
		# .agg(avg("value").alias("average")) \
		# if 'channel' in row:
		# 	logger.info(f"Processing batch {row}")
		collected = batch_df.collect()

		logger.info(f"batchid: {batch_id}")
		# logger.info(f"collected: {collected}")

		for row in collected:
			
			# # Add new data 
			# channel_index = int(row['channel'])
			# new_data[channel_index]['x'].extend([row['timestamp']])
			# new_data[channel_index]['y'].extend([row['value']])
			if row.timestamp < datetime.datetime(2000, 1, 1, 0, 0, 0, 0):
				# Ignore rows that do not have proper timetamp (Not sure where this is coming from.)
				break

			# logger.info({f"row: {row}"})
			# check if value is nan
			if row.value is not None:
				if row.value > 0.00001:
					# for channel_index, _ in enumerate(CHANNEL_NAMES):
					new_data[row.channel]['x'].extend([row.timestamp])
					new_data[row.channel]['y'].extend([row.value])
					logger.info(f"Added to {row.channel} the {[row.timestamp]} and {[row.value]}")

			# raise Exception

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
					
		
		logger.info("Done")
	
		# Update every 5 seconds
		kafka_writer: DataStreamWriter = base_df.writeStream.foreachBatch(plot_data).trigger(processingTime=f'{int(10/args.speed)} seconds')

		kafka_query: StreamingQuery = kafka_writer.start()
		
		kafka_query.awaitTermination()

	msg_process(consumer_conf['bootstrap.servers'],
				args.signal_list)

if __name__ == "__main__":
	main()