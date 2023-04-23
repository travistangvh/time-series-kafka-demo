# Run python bin/plot.py
# Then go to
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure
from bokeh import layouts
from bokeh.models import ColumnDataSource
import numpy as np
import pandas as pd
from collections import defaultdict
from threading import Thread
from bokeh.models import Panel, Tabs, Column, Row, Div
from bokeh.models import Legend
CHANNEL_NAMES =  ['HR', 'RESP', 'PULSE', 'PVC Rate per Minute', 'SpO2', 'CVP', 'ST V', 'NBP Mean', 'NBP Dias', 'NBP Sys']
patient_arr = ['p000194','p044083']
df = pd.read_csv('/Users/michaelscott/bd4h/project/streaming-env/time-series-kafka-demo/explore_output/X.TESTINPUT')
lst = df.iloc[:,2].to_list()

tabs_d = defaultdict(dict)
for pid in patient_arr:
	tabs_d[pid]['original_sources'] = defaultdict(ColumnDataSource)
	tabs_d[pid]['processed_sources'] = defaultdict(ColumnDataSource)
	for channel_index, channel_name in enumerate(CHANNEL_NAMES):
		tabs_d[pid]['original_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
		tabs_d[pid]['processed_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
	tabs_d[pid]['prediction_source'] = ColumnDataSource({'x': [], 'y': []})

def modify_doc(doc):
	# Create a new plot
	# plot = figure()
	# source = ColumnDataSource({'x': [], 'y': []})
	# plot.line('x', 'y', source=source)
	tabs_l = []
	title = Div(text='<h1 style="text-align: left">Dashboard for tracking data</h1>')
	processed_title = Div(text='<h1 style="text-align: left">Preprocessed data</h1>')
	original_title = Div(text='<h1 style="text-align: left">Original data</h1>')
	prediction_title = Div(text='<h1 style="text-align: left">Prediction</h1>')
	for pid in patient_arr:
		# tabs_d[pid]['original_sources'] = defaultdict(ColumnDataSource)
		# tabs_d[pid]['processed_sources'] = defaultdict(ColumnDataSource)
		tabs_d[pid]['original_plots'] = defaultdict(figure)
		tabs_d[pid]['processed_plots'] = defaultdict(figure)
		tabs_d[pid]['original_columns'] = Column()
		tabs_d[pid]['processed_columns'] = Column()
		tabs_d[pid]['legends_d'] = defaultdict(Legend)
		
		# Adding chart for prediction
		# tabs_d[pid]['prediction_source'] = ColumnDataSource({'x': [], 'y': []})
		tabs_d[pid]['prediction_plot'] = figure(toolbar_location=None, name=f'Predictions_fig',
							title=f'Predictions',
							x_axis_type = "datetime",    
								tools="pan,wheel_zoom,box_zoom,reset,save",
								min_width=1600, 
								height=150)
		tabs_d[pid]['prediction_plot'].scatter('x', 'y', source=tabs_d[pid]['prediction_source'], name=f'Predictions_plot')

		# Adding chart for each channel
		for channel_index, channel_name in enumerate(CHANNEL_NAMES):
			# tabs_d[pid]['original_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
			# tabs_d[pid]['processed_sources'][channel_index] = ColumnDataSource({'x': [], 'y': []})
			tabs_d[pid]['original_plots'] [channel_index] = figure(toolbar_location=None, name=f'{channel_name}_fig',
										title=f'{channel_name}',
										x_axis_type = "datetime",    
											tools="pan,wheel_zoom,box_zoom,reset,save",
											min_width=800, 
											height=150)
			# Add original data
			a = tabs_d[pid]['original_plots'][channel_index].scatter('x', 'y', 
							    									 source=tabs_d[pid]['original_sources'][channel_index], 
																	 name=f'{channel_name}_plot')
			
			tabs_d[pid]['processed_plots'] [channel_index] = figure(toolbar_location=None, name=f'{channel_name}_fig',
																	title=f'{channel_name}',
																	x_axis_type = "datetime",    
																		tools="pan,wheel_zoom,box_zoom,reset,save",
																		min_width=800, 
																		height=150)
			# Add jittered data
			b = tabs_d[pid]['processed_plots'][channel_index].scatter('x', 'y', 
																	source=tabs_d[pid]['original_sources'][channel_index], 
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
			tabs_d[pid]['original_columns'].children.append(tabs_d[pid]['original_plots'][channel_index])
			tabs_d[pid]['processed_columns'].children.append(tabs_d[pid]['processed_plots'][channel_index])

		combined_tab = Column(prediction_title,
							  tabs_d[pid]['prediction_plot'],
							  Row(Column(original_title,
		    							 tabs_d[pid]['original_columns']),
								  Column(processed_title,
		 								 tabs_d[pid]['processed_columns'])))
		tabs_l.append(Panel(child=combined_tab, title=str(pid)))


	def update():
		# Update the plot with new data
		# You can add your own code here to retrieve the streaming data
		i = int(np.random.random()*10)
		# print("Update called!")

		# generate a random number
		# new_data = {'x': [np.random()], 'y': [np.random()]}
		for pid in patient_arr:
			prediction_new_data = {'x': [i], 'y': [np.random.random()]}
			tabs_d[pid]['prediction_source'].stream(prediction_new_data)

			for channel_index, _ in enumerate(CHANNEL_NAMES):
				original_new_data = {'x': [i], 'y': [df.iloc[i,channel_index]]}
				processed_new_data = {'x': [i], 'y': [df.iloc[i,channel_index]+np.random.random()]}
				# print(new_data)
				tabs_d[pid]['original_sources'][channel_index].stream(original_new_data)
				tabs_d[pid]['processed_sources'][channel_index].stream(processed_new_data)

	# Add a periodic callback to update the plot every second
	doc.add_periodic_callback(update, 1000)

	# Add the plot to the document
	# doc.add_root(column(plot))
	# for channel_index, channel_name in enumerate(CHANNEL_NAMES):
	# 	doc.add_root(layouts.column(plots[channel_index]))
		

	doc.add_root(Tabs(tabs=tabs_l))

# Create a new Bokeh server application
app = Application(FunctionHandler(modify_doc))
server = Server({'/': app}, port = 5067)
def start_server():

	server.io_loop.start()

# Start the server in a separate thread
server_thread = Thread(target=start_server)
# server_thread.start()
server.io_loop.start()
print("hey, not blocked!")

# https://stackoverflow.com/questions/46754417/why-does-a-running-bokeh-server-display-an-empty-page
# # from utils import get_global_config
# from bokeh.server.server import Server
# from bokeh.application.handlers.function import FunctionHandler
# from bokeh.plotting import figure
# from bokeh.layouts import column
# from bokeh.application import Application
# from bokeh.models import ColumnDataSource
# from bokeh.io import curdoc
# import numpy as np
# import pandas as pd
# from collections import defaultdict


# CHANNEL_NAMES =  ['HR', 'RESP', 'PULSE', 'PVC Rate per Minute', 'SpO2', 'CVP', 'ST V', 'NBP Mean', 'NBP Dias', 'NBP Sys']

# df = pd.read_csv('/Users/michaelscott/bd4h/project/streaming-env/time-series-kafka-demo/explore_output/X.TESTINPUT')
# lst = df.iloc[:,2].to_list()


# def make_document(doc):
#     sources = defaultdict(ColumnDataSource)
#     plots = defaultdict(figure)

#     # Plot 10 lines
#     for channel_name in CHANNEL_NAMES:
#         sources[channel_name] = ColumnDataSource({'x': [], 'y': []})
#         plots[channel_name] = figure(toolbar_location=None, name=f'{channel_name}_fig',
#                                     title=f'{channel_name}',x_axis_type = "linear",    
#                                         tools="pan,wheel_zoom,box_zoom,reset,save",
#                                         min_width=1000, 
#                                         height=150
#                                     )
#         plots[channel_name].line('x', 'y', source=sources[channel_name], name=f'{channel_name}_plot')


#     # Create a new plot
#     # plot = figure(toolbar_location=None)
#     def update():
#         # Update the plot with new data
#         # You can add your own code here to retrieve the streaming data
#         i = int(np.random.random()*10)
#         print("Update called!")

#         # generate a random number
#         # new_data = {'x': [np.random()], 'y': [np.random()]}
#         for channel_idx, channel_name in enumerate(CHANNEL_NAMES):
#             new_data = {'x': [1], 'y': [df.iloc[1,channel_idx]]}
#             print(new_data)
#             sources[channel_name].stream(new_data)
#     # Add the plot to the document
#     for channel_name in CHANNEL_NAMES:
#         doc.add_root(column(plots[channel_name]))


#     i=0
#     # Add a periodic callback to update the plot every second
#     doc.add_periodic_callback(update, 1000)


# apps = {'/': Application(FunctionHandler(make_document))}

# server = Server(apps, port=5008)
# # server.start()
# server.io_loop.start()