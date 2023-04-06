import os
import time
import numpy as np
import torch
import pandas as pd
from torch.utils.data import TensorDataset, Dataset
import torch.nn as nn
import configparser
import socket
from pyspark.sql import SparkSession

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


def get_global_config():
    """Read in all the config variables for the other files to use."""
    config = configparser.ConfigParser()	
    config.read('config.cfg')
    config_vars = {
        "MOUNTPATH": config.get("PATHS", "MOUNTPATH"),
        "DATAPATH": config.get("PATHS", "DATAPATH"),
        "MIMICPATH": config.get("PATHS", "MIMICPATH"),
        "DEMOPATH": config.get("PATHS", "DEMOPATH"),
        "WAVEFPATH": config.get("PATHS", "WAVEFPATH"),
        "OUTPUTPATH": config.get("PATHS", "OUTPUTPATH"),
        "MODELPATH": config.get("PATHS", "MODELPATH"),
        "USE_CUDA": config.getboolean("SETTINGS", "USE_CUDA"),
        "NUM_WORKERS": config.getint("SETTINGS", "NUM_WORKERS"),
        "CHANNEL_NAMES": config.get("SETTINGS", "CHANNEL_NAMES").split(", ")
    }
    return config_vars

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
	with torch.no_grad():
		batch_size = target.size(0)
		# For multiple categories
		# _, pred = output.max(1)

		# For two categories
		pred = torch.round(torch.sigmoid(output))
		correct = pred.eq(target).sum()

		return correct * 100.0 / batch_size

def compute_batch_auc(output, target):
	"""Computes the auc for a batch"""
	from sklearn.metrics import roc_auc_score
	with torch.no_grad():

		batch_size = target.size(0)
		y_pred = torch.sigmoid(output).detach().numpy()[:,1]
		y_true = target.detach().to('cpu').numpy().tolist()

		# print(y_pred)
		# print(y_true)
		auc = roc_auc_score(y_true, y_pred)

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
			y_pred = output.detach().to('cpu').round().long().numpy().tolist()
			# Multiple categories. output is of dimension (batch_size, num_classes)
			# y_pred = output.detach().to('cpu').max(1)[1].numpy().tolist()
			results.extend(list(zip(y_true, y_pred)))

			if i % print_freq == 0:
				print('Test: [{0}/{1}]\t'
					  'Time {batch_time.val:.3f} ({batch_time.avg:.3f})\t'
					  'Loss {loss.val:.4f} ({loss.avg:.4f})\t'
					  'Accuracy {acc.val:.3f} ({acc.avg:.3f})'.format(
					i, len(data_loader), batch_time=batch_time, loss=losses, acc=accuracy))

	return losses.avg, accuracy.avg, results

def train_auc(model, device, data_loader, criterion, optimizer, epoch, print_freq=10):
	batch_time = AverageMeter()
	data_time = AverageMeter()
	losses = AverageMeter()
	auc = AverageMeter()

	model.train()

	end = time.time()
	for i, (input, target) in enumerate(data_loader):
		# measure data loading time
		data_time.update(time.time() - end)

		if isinstance(input, tuple):
			input = tuple([e.to(device) if type(e) == torch.Tensor else e for e in input])
		else:
			input = input.to(device)
		target = target.to(device)

		optimizer.zero_grad()
		output = model(input)
		loss = criterion(output, target)
		assert not np.isnan(loss.item()), 'Model diverged with loss = NaN'

		loss.backward()
		optimizer.step()

		# measure elapsed time
		batch_time.update(time.time() - end)
		end = time.time()

		losses.update(loss.item(), target.size(0))
		auc.update(compute_batch_auc(output, target).item(), target.size(0))

		if i % print_freq == 0:
			print('Epoch: [{0}][{1}/{2}]\t'
				  'Time {batch_time.val:.3f} ({batch_time.avg:.3f})\t'
				  'Data {data_time.val:.3f} ({data_time.avg:.3f})\t'
				  'Loss {loss.val:.4f} ({loss.avg:.4f})\t'
				  'auc {auc.val:.3f} ({auc.avg:.3f})'.format(
				epoch, i, len(data_loader), batch_time=batch_time,
				data_time=data_time, loss=losses, auc=auc))

	return losses.avg, auc.avg

def evaluate_auc(model, device, data_loader, criterion, print_freq=10):
	batch_time = AverageMeter()
	losses = AverageMeter()
	auc = AverageMeter()

	results = []

	model.eval()

	with torch.no_grad():
		end = time.time()
		for i, (input, target) in enumerate(data_loader):

			if isinstance(input, tuple):
				input = tuple([e.to(device) if type(e) == torch.Tensor else e for e in input])
			else:
				input = input.to(device)
			target = target.to(device)

			output = model(input)
			loss = criterion(output, target)

			# measure elapsed time
			batch_time.update(time.time() - end)
			end = time.time()

			losses.update(loss.item(), target.size(0))
			auc.update(compute_batch_auc(output, target).item(), target.size(0))

			y_true = target.detach().to('cpu').numpy().tolist()
			y_pred = torch.sigmoid(output).detach().numpy()[:,1]
			# print(y_pred)
			results.extend(list(zip(y_true, y_pred>0.5)))

			if i % print_freq == 0:
				print('Test: [{0}/{1}]\t'
					  'Time {batch_time.val:.3f} ({batch_time.avg:.3f})\t'
					  'Loss {loss.val:.4f} ({loss.avg:.4f})\t'
					  'Accuracy {auc.val:.3f} ({auc.avg:.3f})'.format(
					i, len(data_loader), batch_time=batch_time, loss=losses, auc=auc))

	return losses.avg, auc.avg, results

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