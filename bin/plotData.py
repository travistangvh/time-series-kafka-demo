# Implement this https://stackoverflow.com/questions/63589249/plotly-dash-display-real-time-data-in-smooth-animation

"""Plots streamed data using Bokeh to http://localhost:5068/."""
# Pyspark imports
import argparse
from confluent_kafka import Consumer, Producer
from pyspark.sql.functions import *
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from utils import acked, get_consumer_config, get_producer_config, build_spark_session, get_global_config

# Bokeh imports
from bokeh.server.server import Server
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure
from bokeh import layouts 
from bokeh.application import Application
from bokeh.server.server import Server
from bokeh.io import curdoc
from collections import defaultdict
from threading import Thread
from bokeh.models import TabPanel, Tabs, Column, Row, Div, Legend, ColumnDataSource
from tornado.ioloop import IOLoop

# Misc Imports
import logging
import asyncio
import datetime
import threading
import mysql.connector
import numpy as np
import pandas as pd
from collections import defaultdict
import time


cfg = get_global_config()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# Define MySQL connection
cnx = mysql.connector.connect(user='root', 
							  password='mauFJcuf5dhRMQrjj',
							  host='172.18.0.8', 
							  database='mydb')

# A mutex to synchronize access to the shared variable
mutex = threading.Lock()

def main():
	"""Create SparkSession."""
	spark = build_spark_session()

	"""Get arguments from command line"""
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument('--signal-list', 
						type=str,
						nargs="*",
						default=list(map(lambda x:x.replace(" ","_"), cfg["CHANNEL_NAMES"])),
						help='List of the Kafka topic to receive unprocessed data.')
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

	# Hardcoding patients to be displayed
	patient_arr = ['p000194','p044083']

	# Function that creates the Bokeh application and define its apperance
	def start_server():
		global tabs_d		
		tabs_d = defaultdict(dict) 
		# you cannot leave this outside because once you do that this item is actually 
		# created outside but only modified inside.
		# the changes made within this function will not be seen outside
		def modify_doc(doc):
			global tabs_d
			logger.info("User has opened http://localhost:5068/!")
			
			tabs_l = []
			title = Div(text='<h1 style="text-align: left">Dashboard for tracking data</h1>')
			processed_title = Div(text='<h1 style="text-align: left">Preprocessed data</h1>')
			original_title = Div(text='<h1 style="text-align: left">Original data</h1>')
			prediction_title = Div(text='<h1 style="text-align: left">Prediction</h1>')

			# Initializing data sources and plots for each patient
			for pid in patient_arr:
				tabs_d[pid]= defaultdict(defaultdict)
				tabs_d[pid]['new_original_sources'] = defaultdict(ColumnDataSource)
				tabs_d[pid]['new_processed_sources'] = defaultdict(ColumnDataSource)
				tabs_d[pid]['original_sources'] = defaultdict(ColumnDataSource)
				tabs_d[pid]['processed_sources'] = defaultdict(ColumnDataSource)
				tabs_d[pid]['prediction_source'] = ColumnDataSource({'x': [], 'y': []})
				tabs_d[pid]['original_plots'] = defaultdict(figure)
				tabs_d[pid]['processed_plots'] = defaultdict(figure)

				tabs_d[pid]['legends_d'] = defaultdict(Legend)

				# Adding chart for prediction
				prediction_plot = figure(toolbar_location=None, name=f'Predictions_fig',
									title=f'Predictions',
									x_axis_type = "datetime",    
										tools="pan,wheel_zoom,box_zoom,reset,save",
										min_width=1600, 
										height=150)
				prediction_plot.scatter('x', 'y', source=tabs_d[pid]['prediction_source'], name=f'Predictions_plot')

				original_column = Column()
				processed_column = Column()

				# Adding chart for each channel
				for channel_index, channel_name in enumerate(cfg['CHANNEL_NAMES']):
					tabs_d[pid]['original_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
					tabs_d[pid]['processed_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
					tabs_d[pid]['new_original_sources'][channel_index] = {'x': [], 'y': []}
					tabs_d[pid]['new_processed_sources'][channel_index] = {'x': [], 'y': []}
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
					# Add Processed data (No data for now)
					b = tabs_d[pid]['processed_plots'][channel_index].scatter('x', 'y', 
																			source=tabs_d[pid]['processed_sources'][channel_index], 
																			name=f'{channel_name}_plot', fill_color='red')
				
					# display legend in top left corner (default is top right corner)
					# plots[channel_index].legend.location = "top_left"

					# Add legends (https://stackoverflow.com/questions/26254619/position-of-the-legend-in-a-bokeh-plot)
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

			def update_data():
				global tabs_d
				
				# Retrieve waveform data from Streaming
				for pid in patient_arr:
					for channel_index, _ in enumerate(cfg['CHANNEL_NAMES']):
						mutex.acquire()
						tabs_d[pid]['original_sources'][channel_index].stream(tabs_d[pid]['new_original_sources'][channel_index])
						tabs_d[pid]['processed_sources'][channel_index].stream(tabs_d[pid]['new_processed_sources'][channel_index])

						# Restart data
						tabs_d[pid]['new_processed_sources'][channel_index] = {'x': [], 'y': []}
						tabs_d[pid]['new_original_sources'][channel_index] = {'x': [], 'y': []}
						mutex.release()

				# Read prediction data from MySQL (These are generated from the streaming data.)
				cursor1 = cnx.cursor()
				query1 = "SELECT SUBJECT_ID, PRED_TIME, RISK_SCORE FROM predictions WHERE DATE(PRED_TIME)>= CURRENT_DATE ORDER BY PRED_TIME"
				cursor1.execute(query1)
				rows1 = cursor1.fetchall()

				for row in rows1:
					# logger.info(f'Rows from predictions:{rows1}')
					pid = f'p{row[0]:0>6}'
					pred_time = row[1]
					risk_score = row[2]
					
					mutex.acquire()
					if not pred_time in tabs_d[pid]['prediction_source'].data['x']:
						tabs_d[pid]['prediction_source'].stream({'x':[pred_time], 'y':[risk_score]})
						logger.info(f"Streaming {pred_time}|{risk_score}.")
					mutex.release()
				
			# Add a periodic callback to update the plot every second
			doc.add_periodic_callback(update_data, 1000)

			doc.add_root(Tabs(tabs=tabs_l))

		# Set up the event loop
		asyncio.set_event_loop(asyncio.new_event_loop())

		# Create a new Bokeh server application
		app = Application(FunctionHandler(modify_doc))
		server = Server({'/': app}, port=5068) # it is crucial to set port=5068, otherwise it will not work

		# Start server (non blocking)
		server.start()
		logger.info("Please manually start a browser http://localhost:5068/ else the server will not work.")

		# Start the server (This is a blocking call)
		IOLoop.current().start()
		
		# Any code after this will not be executed
		# logger.info("ended server")

	# Define the function to preprocess and send the data to Kafka
	def plot_data(batch_df, batch_id):
		"""The function reads in data from sendstream and sends it to Bokeh"""

		global tabs_d

		# Ensuring that the shared varaible can be read.
		mutex.acquire()
		# print(tabs_d)
		mutex.release()
		
		# Collect the data
		collected = batch_df.collect()

		logger.info(f"plotData batchid: {batch_id}")

		# Iterating through all rows
		for row in collected:
			if row.timestamp < datetime.datetime(2000, 1, 1, 0, 0, 0, 0):
				# Ignore rows that do not have proper timetamp (Not sure where this is coming from.)
				break

			# Ignore null values
			if row.value is not None:
				if row.value > 0.00001:
					# replace all ' in row.key
					pid = row.key
					
					# logger.info(f"{pid} | {row.channel} | {[row.timestamp]} | {[row.value]}")

					# Acquire the mutex and update values
					mutex.acquire()
					tabs_d[row.key]['new_original_sources'][row.channel]['x'].extend([row.timestamp])
					tabs_d[row.key]['new_original_sources'][row.channel]['y'].extend([row.value])
					tabs_d[row.key]['new_processed_sources'][row.channel]['x'].extend([row.timestamp])
					tabs_d[row.key]['new_processed_sources'][row.channel]['y'].extend([row.value])
					mutex.release()

			# raise Exception

	def plot_with_bokeh(server, topics):
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
		kafka_writer: DataStreamWriter = base_df.writeStream.foreachBatch(plot_data).trigger(processingTime=f'{int(20/args.speed)} seconds')

		kafka_query: StreamingQuery = kafka_writer.start()
		
		kafka_query.awaitTermination()

	# Start the Visualization Server
	mutex.acquire()
	server_thread = Thread(target=start_server)
	server_thread.start()
	mutex.release()

	# Allow users 60 seconds to open up http://www.localhost:5068
	time.sleep(60)

	# Start the thread that plots the data with Bokeh
	plot_thread = Thread(target=plot_with_bokeh, args=(consumer_conf['bootstrap.servers'], args.signal_list))
	plot_thread.start()

	server_thread.join()
	plot_thread.join()
	
if __name__ == "__main__":
	main()