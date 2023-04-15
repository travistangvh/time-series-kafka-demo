import torch
import torch.nn as nn


class MyCNN(nn.Module):
	def __init__(self):
		# self.MAGIC_NUMBER = 16*41
		self.MAGICNUM = 25
		super(MyCNN, self).__init__()
		self.conv1 = nn.Conv1d(in_channels=10, out_channels=4, kernel_size=10)
		self.conv2 = nn.Conv1d(in_channels=4, out_channels=1, kernel_size=5)
		self.pool = nn.MaxPool1d(kernel_size=3, stride=2)
		self.out1 = nn.Linear(in_features=567, out_features=1)

		self.dropout = nn.Dropout(0.1)
		self.lstm = nn.LSTM(input_size = self.MAGICNUM, hidden_size=16, num_layers=2)
		self.out = nn.Linear(in_features=16, out_features=1, bias=True)
		self.out2 = nn.Linear(in_features=16, out_features=1, bias=True)

		self.age_fn = nn.Linear(in_features=1, out_features=1, bias=True)

	def forward(self, x, age):
		x = torch.tanh(self.conv1(x))
		x = self.pool(x)
		x=self.dropout(x)
		x = torch.tanh(self.conv2(x))
		x = self.pool(x)
		x = self.dropout(x)
		x = x.view(-1, self.MAGICNUM)
		x,_ = self.lstm(x)
		x = self.out(x)
		age_scale = torch.relu(age.unsqueeze(1) * 0.00000001 + 1)
		x = x * age_scale
		x = x.squeeze(1) 
		
		return x