import yfinance as yf
import pandas as pd
import statsmodels.api as sm
import datetime as dt
import os
from statsmodels.tsa.stattools import adfuller
from sklearn.metrics import mean_squared_error
import statsmodels.tsa.stattools as ts
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from ib_insync import *
import time
import csv
import pytz

class account:
	def __init__(self):
		self.ib = self.connect()
	def connect(self):
		i = 0
		while True:
			i += 1
			try:
				ib = IB()
				ib.disconnect()
				ib.connect(clientId=i)
				break
			except:
				continue
		print("Connected",i)
		return ib
	def disconnect(self):
		self.ib.disconnect()
		return
	def run(self):
		print("Starting the main thread")
		self.ib.run()

class pair:
	def __init__(self,tickers,account):
		self.ib_tickers = tickers[0:2]
		self.exchange = tickers[2:4]
		self.primaryExchange = tickers[4:6]
		self.lookback = int(tickers[6])
		self.quantity = int(tickers[7])
		self.amount = 100000
		self.model = None
		self.beta = None
		self.se = None
		self.ib = account.ib
		self.positions = {self.ib_tickers[0]: 0, self.ib_tickers[1] : 0}
		self.prices = {self.ib_tickers[0]: 0, self.ib_tickers[1] : 0}
		self.contract_y = None
		self.contract_x = None
		self.stream_y = None
		self.stream_x = None
		self.flag_y = False
		self.flag_x = False
		self.direction = ""
		self.data_x = []
		self.data_y = []
		self.tz = pytz.timezone('Europe/Dublin')

	def on_new_bar_y(self,bars: BarDataList, has_new_bar: bool):
		if has_new_bar:
			# print(bars[-1].close)
			self.prices[self.ib_tickers[0]] = bars[-1].close
			self.flag_y = True
			self.data_y = bars[-self.lookback:]
			print("Data Recieved",self.ib_tickers,"y", dt.datetime.now())
			self.strategy()	
	def on_new_bar_x(self,bars: BarDataList, has_new_bar: bool):
		if has_new_bar:
			# print(bars[-1].close)
			self.prices[self.ib_tickers[1]] = bars[-1].close
			self.flag_x = True
			self.data_x = bars[-self.lookback:]
			print("Data Recieved",self.ib_tickers,"x", dt.datetime.now())
			self.strategy()

	def config(self):
		self.contract_y = self.get_contracts(self.ib_tickers[0],self.exchange[0],self.primaryExchange[0])
		self.contract_x = self.get_contracts(self.ib_tickers[1],self.exchange[1],self.primaryExchange[1])
		self.stream_y = self.ib.reqHistoricalData(
				self.contract_y,
				endDateTime = '',
				durationStr = "2 D",
				barSizeSetting = "1 min",
				whatToShow='MIDPOINT',
				useRTH=True,
				keepUpToDate=True,
				)

		self.stream_x = self.ib.reqHistoricalData(
				self.contract_x,
				endDateTime = '',
				durationStr = "2 D",
				barSizeSetting = "1 min",
				whatToShow='MIDPOINT',
				useRTH=True,
				keepUpToDate=True,
				)
		self.ib.positionEvent += self.get_positions
		self.stream_y.updateEvent += self.on_new_bar_y
		self.stream_x.updateEvent += self.on_new_bar_x
		# self.positions[self.underlying] = 0
		for i in self.ib.positions():
			if i.contract == self.contract_y:
				self.positions[self.ib_tickers[0]] = i.position
			if i.contract == self.contract_x:
				self.positions[self.ib_tickers[1]] = i.position
		print(self.positions)
		self.model = self.get_model()
		self.beta = self.get_beta()
		self.se = self.get_se()
		print(self.beta)


	def get_contracts(self,conId,exch,pExch):
		contract = None
		if conId[0:2] == "IB":
			contract = CFD(conId)
		elif pExch != ' ':
			contract = Stock(conId,exch,
				primaryExchange = pExch)
		else :
			contract = Stock(conId,exch)
		print(type(contract))
		self.ib.qualifyContracts(contract)
		return contract

	def linear_regression(self,d):
		y = np.array(d.Y)
		x = np.array(d.X).reshape(-1,1)
		model = LinearRegression()
		model.fit(x,y)
		ypred = model.predict(x)
		residuals = y - ypred
		res = (ypred - y)**2
		se = res.mean()**0.5
		return model,se

	def get_model(self):
		# print(data)
		try:
			x_data = self.ib.reqHistoricalData(
				self.contract_x,
				endDateTime = '',
				durationStr = "1 Y",
				barSizeSetting = "1 day",
				whatToShow='MIDPOINT',
				useRTH=True,
				keepUpToDate=False,
				)
			y_data = self.ib.reqHistoricalData(
				self.contract_y,
				endDateTime = '',
				durationStr = "1 Y",
				barSizeSetting = "1 day",
				whatToShow='MIDPOINT',
				useRTH=True,
				keepUpToDate=False,
				)
			close_x = list(map(lambda v : v.close,x_data)) 
			close_y = list(map(lambda v : v.close,y_data))
			d = pd.DataFrame()
			d["Y"] = close_y
			d["X"] = close_x
			d = d.tail(200)
			print(d)
			m,se = self.linear_regression(d)
			return (m,se)
		except:
			print("Error")
			return None

	def get_beta(self):
		if self.model!= None:
			return self.model[0].coef_
		return None

	def get_se(self):
		if self.model!=None:
			return self.model[1]
		return None

	def get_positions(self,pos: Position):
		if pos.contract == self.contract_y:
			self.positions[self.ib_tickers[0]] = pos.position
		if pos.contract == self.contract_x:
			self.positions[self.ib_tickers[1]] = pos.position

	def strategy(self):
		ub = 2.0
		lb = -2.0
		s_ub = 1.0
		s_lb = -1.0
		if self.flag_y and self.flag_x:
			self.flag_y = False
			self.flag_x = False
			if self.model != None:
				# print(dt.datetime.now(self.tz))
				y = self.prices[self.ib_tickers[0]]
				x = self.prices[self.ib_tickers[1]]
				# print(y,x)
				ypred = self.model[0].predict(np.array([x]).reshape(-1,1))
				trig = (y - ypred)/self.se
				res = trig[0]
				posy = self.positions[self.ib_tickers[0]]
				posx = self.positions[self.ib_tickers[1]]
				print(self.ib_tickers[0],self.ib_tickers[1]," Residual: ",res," Positions: ",posy,posx," Prices: ",y,x)
				curr_time = dt.datetime.now(self.tz)
				print(curr_time)
				if curr_time.hour >=19 and curr_time.minute >=45:	
					if posy==0 and posx==0:
						if res>=ub:
							print("SHORT")
							self.direction = "S"
							# order1 = MarketOrder("SELL",self.quantity)
							# order2 = MarketOrder("BUY",int(self.quantity*(y/x)))
							order1 = MarketOrder("SELL",int(self.amount/y))
							order2 = MarketOrder("BUY",int(self.amount/x))
							self.ib.placeOrder(self.contract_y,order1)
							self.ib.placeOrder(self.contract_x,order2)


						elif res<=lb:
							print("LONG")
							self.direction = "L"
							# order1 = MarketOrder("BUY",self.quantity)
							# order2 = MarketOrder("SELL",int(self.quantity*(y/x)))
							order1 = MarketOrder("BUY",int(self.amount/y))
							order2 = MarketOrder("SELL",int(self.amount/x))
							self.ib.placeOrder(self.contract_y,order1)
							self.ib.placeOrder(self.contract_x,order2)

					else:
						if res <= s_ub and self.direction=="S":
							print("SQUARE OFF SHORT")
							self.direction = ""
							if posy < 0:
								order1 = MarketOrder("BUY",-posy)
								self.ib.placeOrder(self.contract_y,order1)
							if posx > 0:
								order2 = MarketOrder("SELL",posx)
								self.ib.placeOrder(self.contract_x,order2)

						elif res >=s_lb and self.direction=="L":
							self.direction=""
							print("SQUARE OFF LONG")
							if posy > 0:
								order1 = MarketOrder("SELL",posy)
								self.ib.placeOrder(self.contract_y,order1)
							if posx < 0:
								order2 = MarketOrder("BUY",-posx)
								self.ib.placeOrder(self.contract_x,order2)

				filename = "./logs/"+self.ib_tickers[0] + "_" + self.ib_tickers[1] + ".csv"
				with open(filename,"a") as f:
					writer = csv.writer(f)
					writer.writerow([dt.datetime.now(self.tz),y,x,res])
					f.close()



def start(n):
	acc = account()
	acc.connect()
	tickers = pd.read_csv("tickers.csv")
	tickers = tickers.values.tolist()
	pairs = []
	for t in tickers:
		print(t)
		p = pair(t,acc)
		p.config()
		pairs.append(p)
		print("Pair object created")
	acc.run()

