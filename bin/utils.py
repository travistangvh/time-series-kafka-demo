

from torch.utils.data import TensorDataset, Dataset
import torch.nn as nn
import os
import time
import numpy as np
import torch
import pandas as pd
import configparser
import socket
from pyspark.sql import SparkSession
import datetime
from sys import platform
import wfdb

def get_global_config():
	"""Read in all the config variables for the other files to use."""
	config = configparser.ConfigParser()	
	if platform == "linux" or platform == "linux2":
		config.read('config.cfg')
		config_vars = {
			"MOUNTPATH": config.get("PATHS", "MOUNTPATH"),
			"DATAPATH": config.get("PATHS", "DATAPATH"),
			"MIMICPATH": config.get("PATHS", "MIMICPATH"),
			"DEMOPATH": config.get("PATHS", "DEMOPATH"),
			"WAVEFPATH": config.get("PATHS", "WAVEFPATH"),
			"OUTPUTPATH": config.get("PATHS", "OUTPUTPATH"),
			"MODELPATH": config.get("PATHS", "MODELPATH"),
			"EXPLOREPATH": config.get("PATHS", "EXPLOREPATH"),
			"USE_CUDA": config.getboolean("SETTINGS", "USE_CUDA"),
			"NUM_WORKERS": config.getint("SETTINGS", "NUM_WORKERS"),
			"CHANNEL_NAMES": config.get("SETTINGS", "CHANNEL_NAMES").split(", "),
			"WINDOWSIZE": config.getint("SETTINGS", "WINDOWSIZE"),
			"RECORDOVERLAP": config.getfloat("SETTINGS", "RECORDOVERLAP"),
			"BATCHSIZE": config.getint("SETTINGS", "BATCHSIZE"),
			}
		
	elif platform == "darwin":
		config.read('../config.cfg')
		#mac
		config_vars = {
			"MOUNTPATH": config.get("PATHS", "LOCALMOUNTPATH"),
			"DATAPATH": config.get("PATHS", "LOCALDATAPATH"),
			"MIMICPATH": config.get("PATHS", "LOCALMIMICPATH"),
			"DEMOPATH": config.get("PATHS", "LOCALDEMOPATH"),
			"WAVEFPATH": config.get("PATHS", "LOCALWAVEFPATH"),
			"OUTPUTPATH": config.get("PATHS", "LOCALOUTPUTPATH"),
			"MODELPATH": config.get("PATHS", "LOCALMODELPATH"),
			"EXPLOREPATH": config.get("PATHS", "LOCALEXPLOREPATH"),
			"USE_CUDA": config.getboolean("SETTINGS", "USE_CUDA"),
			"NUM_WORKERS": config.getint("SETTINGS", "NUM_WORKERS"),
			"CHANNEL_NAMES": config.get("SETTINGS", "CHANNEL_NAMES").split(", "),
			"WINDOWSIZE": config.getint("SETTINGS", "WINDOWSIZE"),
			"RECORDOVERLAP": config.getfloat("SETTINGS", "RECORDOVERLAP"),
			"BATCHSIZE": config.getint("SETTINGS", "BATCHSIZE")
		}
		
	elif platform == "win32":
		# Windows...
		pass
	
	return config_vars

cfg = get_global_config()

def build_spark_session():
	# Option 1. Download the following files into ./jars/ from Maven repo
	# So there is no need to download them everytime it starts
	# Ensure these files into './jars/' from Github repo
	# jars_dir = '/root/.ivy2/jars/'
	# jars = ','.join([f'{jars_dir}{jar}' for jar in [
	# 	'com.github.luben_zstd-jni-1.4.8-1.jar',
	# 	'commons-pool2-2.6.2.jar',
	# 	'kafka-clients-2.6.0.jar',
	# 	'lz4-java-1.7.1.jar',
	# 	'org.apache.commons_commons-pool2-2.6.2.jar',
	# 	'org.apache.kafka_kafka-clients-2.6.0.jar',
	# 	'org.apache.spark_spark-sql-kafka-0-10_2.12-3.1.2.jar',
	# 	'org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.1.2.jar',
	# 	'org.lz4_lz4-java-1.7.1.jar',
	# 	'org.slf4j_slf4j-api-1.7.30.jar',
	# 	'org.spark-project.spark_unused-1.0.0.jar',
	# 	'org.xerial.snappy_snappy-java-1.1.8.2.jar',
	# 	'slf4j-api-1.7.30.jar',
	# 	'snappy-java-1.1.8.2.jar',
	# 	'spark-sql-kafka-0-10_2.12-3.1.2.jar',
	# 	'spark-token-provider-kafka-0-10_2.12-3.1.2.jar',
	# 	'unused-1.0.0.jar',
	# 	'zstd-jni-1.4.8-1.jar'
	# ]])
	# return SparkSession.builder.appName('app').config('spark.jars', jars).getOrCreate()

	# #Option 2: Dynamically download file (slower but works)
	return SparkSession.builder.appName('app').config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()



class AverageMeter(object):
	"""Computes and stores the average and current value"""

	def __init__(self):
		self.reset()

	def reset(self):
		self.val = 0
		self.avg = 0
		self.sum = 0
		self.count = 0

	def update(self, val, n=1):
		self.val = val
		self.sum += val * n
		self.count += n
		self.avg = self.sum / self.count
		
def compute_batch_accuracy(output, target):
	
	"""Computes the accuracy for a batch"""
	# Convert target to long
	with torch.no_grad():
		batch_size = target.size(0)
		# For multiple categories
		# _, pred = output.max(1)

		# For two categories
		pred = torch.sigmoid(output).round().long()
		correct = pred.eq(target).sum()
		return correct * 100.0 / batch_size


# from sklearn.metrics import f1_score
# def compute_batch_accuracy(output, target):
# 		pred = torch.sigmoid(output).round().long()
# 		# Compare pytorch tensor pred with pytorch tensor of 1
# 		f1_score = f1_score(target.cpu().data, pred.cpu())
# 		return torch.tensor([f1_score])



# def compute_batch_accuracy(output, target):
# 		pred = torch.sigmoid(output).round().long()
# 		# Compare pytorch tensor pred with pytorch tensor of 1
# 		tp = ((pred == 1) & (target == 1)).sum()
# 		tn = ((pred == 0) & (target == 0)).sum()
# 		fp = ((pred == 1) & (target == 0)).sum()
# 		fn = ((pred == 0) & (target == 1)).sum()
# 		if (tp+fp)>0 and tp+fn>0: 
# 			precision = tp / (tp + fp)
# 			recall = tp / (tp + fn)
# 			f1 = 2 * (precision * recall) / (precision + recall)
# 			return torch.tensor([f1])
# 		else:
# 			# zero division
# 			# Create an empty tensor of size (3, 4, 5)
# 			res = torch.empty((1))

# 			# Fill the tensor with null values
# 			res.fill_(float("nan"))
# 			return res


# def compute_batch_auc(output, target):
# 	"""Computes the auc for a batch"""
# 	from sklearn.metrics import roc_auc_score
# 	with torch.no_grad():

# 		batch_size = target.size(0)
# 		y_pred = torch.sigmoid(output).detach().numpy()[:,1]
# 		y_true = target.detach().to('cpu').numpy().tolist()

# 		# print(y_pred)
# 		# print(y_true)
# 		auc = roc_auc_score(y_true, y_pred)

		return auc

def train(model, device, data_loader, criterion, optimizer, epoch, print_freq=10):
	batch_time = AverageMeter()
	data_time = AverageMeter()
	losses = AverageMeter()
	accuracy = AverageMeter()

	model.train()

	end = time.time()
	for i, (input, age, target) in enumerate(data_loader):
		# measure data loading time
		data_time.update(time.time() - end)

		if isinstance(input, tuple):
			input = tuple([e.to(device) if type(e) == torch.Tensor else e for e in input])
		else:
			input = input.to(device)
		age = age.to(device)
		target = target.to(device)

		optimizer.zero_grad()
		output = model(input, age)
		loss = criterion(output, target)
		assert not np.isnan(loss.item()), 'Model diverged with loss = NaN'

		loss.backward()
		optimizer.step()

		# measure elapsed time
		batch_time.update(time.time() - end)
		end = time.time()

		losses.update(loss.item(), target.size(0))
		accuracy.update(compute_batch_accuracy(output, target).item(), target.size(0))

		if i % print_freq == 0:
			print('Epoch: [{0}][{1}/{2}]\t'
				  'Time {batch_time.val:.3f} ({batch_time.avg:.3f})\t'
				  'Data {data_time.val:.3f} ({data_time.avg:.3f})\t'
				  'Loss {loss.val:.4f} ({loss.avg:.4f})\t'
				  'Accuracy {acc.val:.3f} ({acc.avg:.3f})'.format(
				epoch, i, len(data_loader), batch_time=batch_time,
				data_time=data_time, loss=losses, acc=accuracy))

	return losses.avg, accuracy.avg

def evaluate(model, device, data_loader, criterion, print_freq=10):
	batch_time = AverageMeter()
	losses = AverageMeter()
	accuracy = AverageMeter()

	results = []

	model.eval()

	with torch.no_grad():
		end = time.time()
		for i, (input, age, target) in enumerate(data_loader):

			if isinstance(input, tuple):
				input = tuple([e.to(device) if type(e) == torch.Tensor else e for e in input])
			else:
				input = input.to(device)
			age = age.to(device)
			target = target.to(device)

			output = model(input, age)
			loss = criterion(output, target)

			# measure elapsed time
			batch_time.update(time.time() - end)
			end = time.time()

			losses.update(loss.item(), target.size(0))
			accuracy.update(compute_batch_accuracy(output, target).item(), target.size(0))

			y_true = target.detach().to('cpu').numpy().tolist()
			# One category. output is of dimension (batch_Size,)
			y = torch.sigmoid(output).detach().to('cpu')
			y_pred = y.round().long().numpy().tolist()
			y_prob = y.numpy().tolist()
			# Multiple categories. output is of dimension (batch_size, num_classes)
			# y_pred = output.detach().to('cpu').max(1)[1].numpy().tolist()
			results.extend(list(zip(y_true, y_pred, y_prob)))

			if i % print_freq == 0:
				print('Test: [{0}/{1}]\t'
					  'Time {batch_time.val:.3f} ({batch_time.avg:.3f})\t'
					  'Loss {loss.val:.4f} ({loss.avg:.4f})\t'
					  'Accuracy {acc.val:.3f} ({acc.avg:.3f})'.format(
					i, len(data_loader), batch_time=batch_time, loss=losses, acc=accuracy))

	return losses.avg, accuracy.avg, results

# def train_auc(model, device, data_loader, criterion, optimizer, epoch, print_freq=10):
# 	batch_time = AverageMeter()
# 	data_time = AverageMeter()
# 	losses = AverageMeter()
# 	auc = AverageMeter()

# 	model.train()

# 	end = time.time()
# 	for i, (input, target) in enumerate(data_loader):
# 		# measure data loading time
# 		data_time.update(time.time() - end)

# 		if isinstance(input, tuple):
# 			input = tuple([e.to(device) if type(e) == torch.Tensor else e for e in input])
# 		else:
# 			input = input.to(device)
# 		target = target.to(device)

# 		optimizer.zero_grad()
# 		output = model(input)
# 		loss = criterion(output, target)
# 		assert not np.isnan(loss.item()), 'Model diverged with loss = NaN'

# 		loss.backward()
# 		optimizer.step()

# 		# measure elapsed time
# 		batch_time.update(time.time() - end)
# 		end = time.time()

# 		losses.update(loss.item(), target.size(0))
# 		auc.update(compute_batch_auc(output, target).item(), target.size(0))

# 		if i % print_freq == 0:
# 			print('Epoch: [{0}][{1}/{2}]\t'
# 				  'Time {batch_time.val:.3f} ({batch_time.avg:.3f})\t'
# 				  'Data {data_time.val:.3f} ({data_time.avg:.3f})\t'
# 				  'Loss {loss.val:.4f} ({loss.avg:.4f})\t'
# 				  'auc {auc.val:.3f} ({auc.avg:.3f})'.format(
# 				epoch, i, len(data_loader), batch_time=batch_time,
# 				data_time=data_time, loss=losses, auc=auc))

# 	return losses.avg, auc.avg

# def evaluate_auc(model, device, data_loader, criterion, print_freq=10):
# 	batch_time = AverageMeter()
# 	losses = AverageMeter()
# 	auc = AverageMeter()

# 	results = []

# 	model.eval()

# 	with torch.no_grad():
# 		end = time.time()
# 		for i, (input, target) in enumerate(data_loader):

# 			if isinstance(input, tuple):
# 				input = tuple([e.to(device) if type(e) == torch.Tensor else e for e in input])
# 			else:
# 				input = input.to(device)
# 			target = target.to(device)

# 			output = model(input)
# 			loss = criterion(output, target)

# 			# measure elapsed time
# 			batch_time.update(time.time() - end)
# 			end = time.time()

# 			losses.update(loss.item(), target.size(0))
# 			auc.update(compute_batch_auc(output, target).item(), target.size(0))

# 			y_true = target.detach().to('cpu').numpy().tolist()
# 			y_pred = torch.sigmoid(output).detach().numpy()[:,1]
# 			# print(y_pred)
# 			results.extend(list(zip(y_true, y_pred>0.5)))

# 			if i % print_freq == 0:
# 				print('Test: [{0}/{1}]\t'
# 					  'Time {batch_time.val:.3f} ({batch_time.avg:.3f})\t'
# 					  'Loss {loss.val:.4f} ({loss.avg:.4f})\t'
# 					  'Accuracy {auc.val:.3f} ({auc.avg:.3f})'.format(
# 					i, len(data_loader), batch_time=batch_time, loss=losses, auc=auc))

# 	return losses.avg, auc.avg, results

def load_dataset(x,age_arr,y):
	"""
	:param path: a path to the seizure data CSV file
	:return dataset: a TensorDataset consists of a data Tensor and a target Tensor
	"""
	# Casting in pytorch tensor
	data = torch.from_numpy(x).type(torch.FloatTensor)
	target = torch.from_numpy(y).type(torch.FloatTensor) # BCE with logit expects target to be float. CrossEntropy expects long
	age = torch.from_numpy(age_arr).type(torch.FloatTensor)

	# x[torch.isnan(x)] = 0

	# Need to be of size (N, Cn, L)
	# N: batch size
	# Cn: number of channels
	# L: length of the sequence
	data = data.reshape((data.shape[0], data.shape[1], data.shape[2]))
	dataset = TensorDataset(data, age.float(), target.float())

	return dataset

# def load_dataset_production(x_arr, age_arr):
# 	"""
# 	:param path: a path to the seizure data CSV file
# 	:return dataset: a TensorDataset consists of a data Tensor and a target Tensor
# 	"""
# 	# Casting in pytorch tensor
# 	data = torch.from_numpy(x_arr).type(torch.FloatTensor)
# 	age = torch.from_numpy(age_arr).type(torch.FloatTensor)

# 	# x[torch.isnan(x)] = 0

# 	# Need to be of size (N, Cn, L)
# 	# N: batch size
# 	# Cn: number of channels
# 	# L: length of the sequence
# 	# data = data.reshape((data.shape[0], data.shape[1], data.shape[2]))
# 	dataset = TensorDataset(data, age.float())

# 	return dataset

"""
Utility functions for streaming
"""
def acked(err, msg):
	if err is not None:
		print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
	else:
		print("Message produced: %s" % (str(msg.value())))

def get_producer_config():
	return {'bootstrap.servers': "172.18.0.4:29092",
			'client.id': socket.gethostname(),
			'acks':'all', # continuously prints ack every time a message is sent. but slows process down. 
			'retries':5
			}

def get_consumer_config():
	return {'bootstrap.servers': "172.18.0.4:29092",
			'client.id': socket.gethostname(),
			'acks':'all' # continuously prints ack every time a message is sent. but slows process down. 
			}

# Functions for waveform
def get_waveform_path(record_name):
	patient_id = record_name[0:7]
	return cfg['WAVEFPATH'] + f'/{patient_id[0:3]}/{patient_id}/{record_name}'

def get_record(record_name):
	patient_path = get_waveform_path(record_name)
	return wfdb.rdrecord(patient_path, channel_names=cfg['CHANNEL_NAMES'])

def get_base_time(record_name):
	"""Base time is the starting time of a waveform"""
	basetime = record_name[8:]
	basetime = basetime.strip().strip('n')
	return basetime

def get_ending_time(record_name):
	"""Ending time is the end time of a record"""
	# Get waveform data
	record = get_record(record_name)
	
	# # Version1: Retrieve the ending time as the time using basetime + numrecords
	# # get time when recording started
	# base_time = datetime.datetime.combine(record.__dict__['base_date'], 
	#                         record.__dict__['base_time'])
	
	# duration = record.__dict__['sig_len'] * (1/record.__dict__['fs']) 

	# # Add duration to base time
	# end_time = base_time + datetime.timedelta(seconds=duration)
	
	# # Version 2: Retrieve the ending time as the final valid index in the dataframe
	# I think this is more accurate. We'll go with this.
	record_df = record.to_dataframe()
	
	end_time = record_df.last_valid_index().to_pydatetime()

	record_present = [0 for i in range(len(cfg['CHANNEL_NAMES']))]

	# One hot encode the presence of signals
	for idx, val in enumerate(cfg['CHANNEL_NAMES']):
		if val in record.__dict__['sig_name']:
			record_present[idx] = 1

	return end_time, record_present

def plot_waveform(record_name = 'p087675-2104-12-05-03-53n', ca_time_str = '2104-12-05 08:40:00'):
	# Get waveform data
	patient_path = get_waveform_path(record_name)
	record = wfdb.rdrecord(patient_path, channel_names=cfg['CHANNEL_NAMES'])

	# Select only HR from the plots, not with the channels parameter
	fig = wfdb.plot_wfdb(record=record, title=record_name[0:7], figsize=(10,15), return_fig=True)
	ax_list = fig.axes

	# get time when recording started
	base_time = datetime.datetime.combine(record.__dict__['base_date'], 
							record.__dict__['base_time'])

	# Create datetime using strptime
	ca_time = datetime.datetime.strptime(ca_time_str, '%Y-%m-%d %H:%M:%S')

	# find time delta between base_time and ca_time
	# Convert it to an int that represents the number of seconds
	time_delta = int((ca_time - base_time).total_seconds())

	# Plot a vertical line at time_delta
	if time_delta > 0:
		for ax in ax_list:
			ax.axvline(x=time_delta, color='red', linestyle='--',)
	else:
		# change title of fig
		fig.suptitle('Cardiac Arrest Time is before the start of the recording')

	fig.show()
	try:
		# Not necessary in production
		from IPython.display import display
		display(record.__dict__)
	except:
		pass
		

# Helper functions for creating dataset
def create_batch(df, window_size=cfg['WINDOWSIZE'], overlap_pct=cfg['RECORDOVERLAP']):
	# Convert to numpy array of 120 rows each
	# with intersect of 40%
	# Set the window size to 120 and the overlap to 40%
	overlap = int(window_size * overlap_pct)

	# Convert the DataFrame to a numpy array
	data = df.values

	if len(data) == window_size:
		# Convert the list of windows to a numpy array
		# Create a new axis to to specify batch size of 1
		windows = np.array(data)[np.newaxis, :]

	else:
		# Use the rolling method to create the sliding windows
		windows = []
		for i in range(0, len(data) - window_size, window_size - overlap):
			window = data[i:i+window_size]
			windows.append(window)
		
		# Convert the list of windows to a numpy array
		windows = np.array(windows)
		
	# converts from (n, 120, 10) to (n, 10, 120) 
	return np.swapaxes(windows,1,2)

def get_arr(arr, y):
	return np.array([y]*arr.shape[0]).squeeze()

def load_dataset(x,age_arr,y):
	"""
	:param path: a path to the seizure data CSV file
	:return dataset: a TensorDataset consists of a data Tensor and a target Tensor
	"""
	# Casting in pytorch tensor
	data = torch.from_numpy(x).type(torch.FloatTensor)
	target = torch.from_numpy(y).type(torch.FloatTensor) # BCE with logit expects target to be float. CrossEntropy expects long
	age = torch.from_numpy(age_arr).type(torch.FloatTensor)

	# x[torch.isnan(x)] = 0

	# Need to be of size (N, Cn, L)
	# N: batch size
	# Cn: number of channels
	# L: length of the sequence
	data = data.reshape((data.shape[0], data.shape[1], data.shape[2]))
	dataset = TensorDataset(data, age.float(), target.float())

	return dataset


# Running a model with a dummy
# To do: https://pytorch.org/docs/stable/data.html#torch.utils.data.IterableDataset
def run_model_dummy():
	"""A dummy model that takes uses a dummy model and produces dummy predictions."""
	print('Starting prediction...')

	# Defining constants
	SUBJECTID = 70723
	RECORDNAME = 'p070723-2163-11-18-16-28n'
	BASETIME = get_base_time(RECORDNAME)
	ENDTIME, RECORD_PRESENT = get_ending_time(RECORDNAME)
	CATIME = datetime.datetime.strptime('2163-11-20 11:37:20', '%Y-%m-%d %H:%M:%S') # ORIGINALLY 2163-11-18 15:06:00 (TIME OF NOTEEVENTS)
	SPLITTIME = CATIME - datetime.timedelta(hours=2)
	FINISHTIME = SPLITTIME - datetime.timedelta(minutes=5)
	AGE = 60

	# Retrieve record
	record = get_record(RECORDNAME)
	record_df = record.to_dataframe()

	# get the index of the first non-null value in the DataFrame
	actual_BASETIME = record_df.first_valid_index()

	# get actual CA time as the final valid index of the entire dataframe
	# if the patient is dead
	actual_CATIME = record_df.last_valid_index()

	# get actual splittime
	actual_SPLITTIME = actual_CATIME - datetime.timedelta(hours=2)

	actual_FINISHTIME = actual_CATIME - datetime.timedelta(minutes=5)

	# retrieve the first non-null record using the loc method
	record_df = record_df.loc[actual_BASETIME:actual_CATIME]

	# Create one-hot encoding as a way to indicate if a channel is present or not
	# 1 for a channel present, 0 for a channel absent.
	# e.g. cfg['CHANNEL_NAMES'][0] is 0 if HR is absent.
	#       cfg['CHANNEL_NAMES'][1] is 1 if PULSE is present.
	# check config.cfg's CHANNEL_NAMES for index sequence 
	for channel in cfg['CHANNEL_NAMES']:
		if channel not in record_df.columns:
			record_df[channel.replace(' ','_')] = 0

	# perform preprocessing
	record_df = record_df.interpolate(method='linear').fillna(0)

	# Sample at 5 second interval
	# take instantaneous value
	record_df = record_df.resample('5S').first()

	# define positive and negative
	negative_df = record_df.loc[:actual_SPLITTIME]
	positive_df = record_df.loc[actual_SPLITTIME:actual_FINISHTIME,:]

	# Create x, y, and a arr for negative
	x_negative_arr = create_batch(negative_df)
	y_negative_arr = get_arr(x_negative_arr,0)
	a_negative_arr = get_arr(x_negative_arr,AGE)

	# Create x, y, and a arr for positive
	x_positive_arr = create_batch(positive_df)
	y_positive_arr = get_arr(x_positive_arr,1)
	a_positive_arr = get_arr(x_positive_arr,AGE)

	# Concatenating all arrays
	x_arr = np.concatenate([x_negative_arr,x_positive_arr])
	y_arr = np.concatenate([y_negative_arr,y_positive_arr])
	a_arr = np.concatenate([a_negative_arr,a_positive_arr])

	# converts from (n, 120, 10) to (n, 10, 120) 
	x_arr = np.swapaxes(x_arr,1,2)

	# Split between train + test
	from sklearn.model_selection import ShuffleSplit # or StratifiedShuffleSplit
	sss = ShuffleSplit(n_splits=1, test_size=0.3)
	sss.get_n_splits(x_arr, y_arr)
	train_index, valtest_index = next(sss.split(x_arr, y_arr)) 

	X_train, X_valtest = x_arr[train_index], x_arr[valtest_index] 
	a_train, a_valtest = a_arr[train_index], a_arr[valtest_index] 
	y_train, y_valtest = y_arr[train_index], y_arr[valtest_index]

	# Split between val and test
	sss = ShuffleSplit(n_splits=1, test_size=0.5)
	sss.get_n_splits(X_valtest, y_valtest)
	val_index, test_index = next(sss.split(X_valtest, y_valtest)) 

	X_val, X_test = X_valtest[val_index], X_valtest[test_index] 
	a_val, a_test = a_valtest[val_index], a_valtest[test_index] 
	y_val, y_test = y_valtest[val_index], y_valtest[test_index]

	# Loading model
	model = torch.load(cfg['MODELPATH'])

	print('Model loaded')
	test_dataset = load_dataset(X_test,a_test,y_test)
	test_loader = torch.utils.data.DataLoader(test_dataset, batch_size=cfg['BATCHSIZE'], shuffle=False, num_workers=cfg['NUM_WORKERS'], drop_last=True)
	criterion = nn.BCEWithLogitsLoss()
	device = torch.device("cuda" if cfg['USE_CUDA'] and torch.cuda.is_available() else "cpu")
	losses_avg, accuracy_avg, results = evaluate(model, device, test_loader, criterion, print_freq=10)

	print(results)

	return results 

def run_model(model, device, data_df, age):

	# Generate an feature array
	x_arr = create_batch(data_df, window_size=cfg['WINDOWSIZE'], overlap_pct = 0)

	# generate an age array of the same size as x_arr
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

		# print(y_prob)
		# print(y_pred)

	return y_pred, y_prob

