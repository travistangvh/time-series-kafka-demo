# Implement this https://stackoverflow.com/questions/63589249/plotly-dash-display-real-time-data-in-smooth-animation

"""Consumes stream for printing all messages to the console."""

import argparse
from confluent_kafka import Consumer, Producer
from pyspark.sql.functions import *
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from utils import acked, get_consumer_config, get_producer_config, build_spark_session, get_global_config
import logging
import asyncio
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
from bokeh.models import TabPanel, Tabs, Column, Row, Div
from bokeh.models import Legend
import datetime
import threading
import mysql.connector

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

# Define MySQL connection
cnx = mysql.connector.connect(user='root', 
							  password='mauFJcuf5dhRMQrjj',
							  host='172.18.0.8', 
							  database='mydb')


CHANNEL_NAMES =  ['HR', 'RESP', 'PULSE', 'PVC Rate per Minute', 'SpO2', 'CVP', 'ST V', 'NBP Mean', 'NBP Dias', 'NBP Sys']
patient_arr = ['p000194','p044083']
df = pd.read_csv(f'{cfg["EXPLOREPATH"]}/X.TESTINPUT')
lst = df.iloc[:,2].to_list()

# A mutex to synchronize access to the shared variable
mutex = threading.Lock()

# tabs_d = {'p000194': {'original_souces': {}, 'processsed_sources': {} },
# 	  	'p044083': {'original_souces': {}, 'processsed_sources': {}}}


# for pid in patient_arr:



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


	# Define a function to start the I/O loop in a separate thread
	def start_server():
		global tabs_d		
		tabs_d = defaultdict(dict) # you cannot leave this outside because once you do that this item is actually created outside but only modified inside.
											# the changes made within this function will not be seen outside
		def modify_doc(doc):
			global tabs_d
			logger.info("Ive been summouned!")
			# Create a new plot
			# plot = figure()
			# source = ColumnDataSource({'x': [], 'y': []})
			# plot.line('x', 'y', source=source)
			# mutex.acquire()
			
			tabs_l = []
			title = Div(text='<h1 style="text-align: left">Dashboard for tracking data</h1>')
			processed_title = Div(text='<h1 style="text-align: left">Preprocessed data</h1>')
			original_title = Div(text='<h1 style="text-align: left">Original data</h1>')
			prediction_title = Div(text='<h1 style="text-align: left">Prediction</h1>')

			for pid in patient_arr:
				tabs_d[pid]= defaultdict(defaultdict)
			# # for channel_index, channel_name in enumerate(cfg['CHANNEL_NAMES']):
			# 	tabs_d[pid]['new_original_sources'][channel_index] = {'x':[],'y':[]}
			# 	tabs_d[pid]['new_processed_sources'][channel_index] = {'x':[],'y':[]}
				tabs_d[pid]['new_original_sources'] = defaultdict(ColumnDataSource)
				tabs_d[pid]['new_processed_sources'] = defaultdict(ColumnDataSource)
				tabs_d[pid]['original_sources'] = defaultdict(ColumnDataSource)
				tabs_d[pid]['processed_sources'] = defaultdict(ColumnDataSource)
				logger.info("Done!!")
				tabs_d[pid]['prediction_source'] = ColumnDataSource({'x': [], 'y': []})
				# prediction_source = ColumnDataSource({'x': [], 'y': []})
				tabs_d[pid]['original_plots'] = defaultdict(figure)
				tabs_d[pid]['processed_plots'] = defaultdict(figure)

				tabs_d[pid]['legends_d'] = defaultdict(Legend)

				original_column = Column()
				processed_column = Column()
				# Adding chart for prediction
				prediction_plot = figure(toolbar_location=None, name=f'Predictions_fig',
									title=f'Predictions',
									x_axis_type = "datetime",    
										tools="pan,wheel_zoom,box_zoom,reset,save",
										min_width=1600, 
										height=150)
				prediction_plot.scatter('x', 'y', source=tabs_d[pid]['prediction_source'], name=f'Predictions_plot')

				# Adding chart for each channel
				for channel_index, channel_name in enumerate(CHANNEL_NAMES):
					tabs_d[pid]['original_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
					tabs_d[pid]['processed_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
					tabs_d[pid]['new_original_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
					tabs_d[pid]['new_processed_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
					tabs_d[pid]['original_plots'][channel_index] = figure(toolbar_location=None, name=f'{channel_name}_fig',
												title=f'{channel_name}',
												x_axis_type = "datetime",    
													tools="pan,wheel_zoom,box_zoom,reset,save",
													min_width=800, 
													height=150)
					# Add original data
					a = tabs_d[pid]['original_plots'][channel_index].scatter('x', 'y', 
																			source=tabs_d[pid]['original_sources'][channel_index], 
																			name=f'{channel_name}_plot')
					
					tabs_d[pid]['processed_plots'][channel_index] = figure(toolbar_location=None, name=f'{channel_name}_fig',
																			title=f'{channel_name}',
																			x_axis_type = "datetime",    
																				tools="pan,wheel_zoom,box_zoom,reset,save",
																				min_width=800, 
																				height=150)
					# Add jittered data
					b = tabs_d[pid]['processed_plots'][channel_index].scatter('x', 'y', 
																			source=tabs_d[pid]['processed_sources'][channel_index], 
																			name=f'{channel_name}_plot', fill_color='red')
				
					

					# display legend in top left corner (default is top right corner)
					# plots[channel_index].legend.location = "top_left"

					# Add legends
					# https://stackoverflow.com/questions/26254619/position-of-the-legend-in-a-bokeh-plot
					# tabs_d[pid]['legends_d'][channel_index] = Legend(items=[
					# 	("Original",   [a]),
					# 	("Processed", [b])
					# ], location=(0, -30))

					# tabs_d[pid]['plots'][channel_index].add_layout(tabs_d[pid]['legends_d'][channel_index],'right')

					# Add the plot to a Column model
					original_column.children.append(tabs_d[pid]['original_plots'][channel_index])
					processed_column.children.append(tabs_d[pid]['processed_plots'][channel_index])

				combined_tab = Column(prediction_title,
									prediction_plot,
									Row(Column(original_title,
												original_column),
										Column(processed_title,
												processed_column)))
				tabs_l.append(TabPanel(child=combined_tab, title=str(pid)))

			# mutex.release()


			def update_data():
				global tabs_d
				
				# Read data from 
				cursor1 = cnx.cursor()
				query1 = "SELECT SUBJECT_ID, PRED_TIME, RISK_SCORE FROM predictions ORDER BY PRED_TIME"
				cursor1.execute(query1)
				rows1 = cursor1.fetchall()
				final_prediction = '2021-01-01 00:00:00'

				for row in rows1:
					
					logger.info(f'Rows from predictions:{rows1}')
					pid = f'p{row[0]:0>6}'
					pred_time = row[1]
					risk_score = row[2]
					
					mutex.acquire()
					tabs_d[pid]['prediction_source'].stream({'x':[pred_time], 'y':[risk_score]})
					mutex.release()
				
					
				for pid in patient_arr:
					for channel_index, _ in enumerate(cfg['CHANNEL_NAMES']):
						mutex.acquire()
						tabs_d[pid]['original_sources'][channel_index].stream(tabs_d[pid]['new_original_sources'][channel_index].data)
						tabs_d[pid]['processed_sources'][channel_index].stream(tabs_d[pid]['new_processed_sources'][channel_index].data)
						mutex.release()



						# tabs_d[pid]['new_original_sources'][channel_index].data = {'x':[],'y':[]}
						# tabs_d[pid]['new_processed_sources'][channel_index].data = {'x':[],'y':[]}
				# # Update the plot with new data
				# # You can add your own code here to retrieve the streaming data
				# i = int(np.random.random()*10)
				# # print("Update called!")

				# # generate a random number
				# # new_data = {'x': [np.random()], 'y': [np.random()]}
				# for pid in patient_arr:
				# 	prediction_new_data = {'x': [i], 'y': [np.random.random()]}
				# 	tabs_d[pid]['prediction_source'].stream(prediction_new_data)
				# 	if 'original_sources' not in tabs_d[pid]:
				# 		tabs_d[pid]['original_sources'] = defaultdict(ColumnDataSource)
				# 		for channel_index, channel_name in enumerate(CHANNEL_NAMES):
				# 			tabs_d[pid]['original_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
				# 		return
				# 	if 'processed_sources' not in tabs_d[pid]:
				# 		tabs_d[pid]['processed_sources'] = defaultdict(ColumnDataSource)
				# 		for channel_index, channel_name in enumerate(CHANNEL_NAMES):
				# 			tabs_d[pid]['processed_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
				# 		return
				# 	for channel_index, _ in enumerate(CHANNEL_NAMES):
				# 		# original_new_data = {'x': [i], 'y': [df.iloc[i,channel_index]]}
				# 		# processed_new_data = {'x': [i], 'y': [df.iloc[i,channel_index]+np.random.random()]}
				# 		# print(new_data)
				# 		if len(tabs_d[pid]['original_sources'][channel_index]['x'])>0:
				# 			# tabs_d[pid]['original_sources'][channel_index].stream(tabs_d[pid]['original_sources'][channel_index])
				# 		if len(tabs_d[pid]['processed_sources'][channel_index]['x'])>0:
				# 			# tabs_d[pid]['processed_sources'][channel_index].stream(tabs_d[pid]['processed_sources'][channel_index])
				# for pid in patient_arr:
				# 	tabs_d[pid]['processed_plots'][channel_index].data_source.trigger('data', tabs_d[pid]['processed_plots'][channel_index].data_source.data, 
				# 					 										   				tabs_d[pid]['processed_sources'][channel_index].data)
			# Add a periodic callback to update the plot every second
			doc.add_periodic_callback(update_data, 1000)

			# Add the plot to the document
			# doc.add_root(column(plot))
			# for channel_index, channel_name in enumerate(CHANNEL_NAMES):
			# 	doc.add_root(layouts.column(plots[channel_index]))
				

			doc.add_root(Tabs(tabs=tabs_l))

		# Set up the event loop
		asyncio.set_event_loop(asyncio.new_event_loop())

		# Create a new Bokeh server application
		app = Application(FunctionHandler(modify_doc))
		server = Server({'/': app}, port=5068) # it is crucial to set port=5068, otherwise it will not work
		# server.io_loop.start()

		server.start()
		logger.info("started server. Please manually start a browser http://localhost:5068/ else the server will not work.")


		# import os
		# os.system("xdg-open \"\" http://localhost:5068/")

		import webbrowser
		webbrowser.open('http://localhost:5068/', new = 2)

		# Please manually start a browser http://localhost:5068/
		# from bokeh.server.session import Session
		# # Create a new session
		# session = Session()
		# session.connect("localhost:5068")

		# # Use the session to modify a document
		# doc = session.document
		# doc.title = "My App - Session 1"

		# server.run_until_shutdown()
		from tornado.ioloop import IOLoop

		IOLoop.current().start()
		
		logger.info("ended server")

		# Write

	# Define the function to preprocess and send the data to Kafka
	def plot_data(batch_df, batch_id):
		global tabs_d

		mutex.acquire()
		print(tabs_d)
		mutex.release()
		
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

		logger.info(f"plotData batchid: {batch_id}")
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
					# replace all ' in row.key
					pid = row.key.replace("'", "")
					logger.info(f"row: {row}")
					logger.info(f"row: {row.key}") # p000194
					# if row.key == "p000194":
					# 	print("Hey, I'm here!")
					# else:
					# 	print('LOL')
					logger.info(f"row: {pid}")
					# for channel_index, _ in enumerate(CHANNEL_NAMES):
					# if 'original_sources' not in tabs_d[pid]:
					# 	tabs_d[pid]['original_sources'] = defaultdict(ColumnDataSource)
					# 	for channel_index, channel_name in enumerate(CHANNEL_NAMES):
					# 		tabs_d[pid]['original_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
					# 		tabs_d[pid]['processed_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
					logger.info(f"{pid} | {row.channel} | {[row.timestamp]} | {[row.value]}")
					mutex.acquire()
					tabs_d['p000194']['new_original_sources'][row.channel].data['x'].extend([row.timestamp])
					tabs_d['p000194']['new_original_sources'][row.channel].data['y'].extend([row.value])
					tabs_d['p000194']['new_processed_sources'][row.channel].data['x'].extend([row.timestamp])
					tabs_d['p000194']['new_processed_sources'][row.channel].data['y'].extend([row.value])
					mutex.release()

			# raise Exception

	def msg_process(server, topics):
		"""Create a streaming dataframe that takes in values of messages received, 
		together with the current timestamp.
		Then, print them out.
		Then, process the message in batch
		Reference link: https://medium.com/@aman.parmar17/handling-real-time-kafka-data-streams-using-pyspark-8b6616a3a084"""
		global tabs_d
		mutex.acquire()
		print(tabs_d)
		mutex.release()
		df = (spark.
				readStream
				.format("kafka")
				.option("kafka.bootstrap.servers", server)
				.option("subscribe", ",".join(topics))
				.option("startingOffsets", "latest")
				.load()
			)
		
		# Select the value and timestamp (the message is received)
		base_df = df.selectExpr("REPLACE(CAST(key as STRING),\"'\",'') as key",
								"CAST(REPLACE(replace(substring_index(CAST(value as STRING), ',' ,1),'[',''), '\"', '')  AS INT) as channel",
								"CAST(replace(substring_index(CAST(value as STRING), ',' ,-1),']','') as FLOAT) as value",
								# "date_format(timestamp,'HH:mm:ss') as time",
								"CAST(timestamp as TIMESTAMP) as timestamp")
					
		
		logger.info("Done")
	
		# Update every 5 seconds
		kafka_writer: DataStreamWriter = base_df.writeStream.foreachBatch(plot_data).trigger(processingTime=f'{int(10/args.speed)} seconds')

		kafka_query: StreamingQuery = kafka_writer.start()
		
		kafka_query.awaitTermination()


	# Start the Visualization Server
	mutex.acquire()
	server_thread = Thread(target=start_server)
	server_thread.start()
	logger.info("unblocked!")
	mutex.release()

	time.sleep(60)
	# Start the messaging server
	messaging_thread = Thread(target=msg_process, args=(consumer_conf['bootstrap.servers'], args.signal_list))
	messaging_thread.start()

	server_thread.join()
	messaging_thread.join()
	# msg_process(consumer_conf['bootstrap.servers'],
	# 			args.signal_list)

if __name__ == "__main__":
	main()