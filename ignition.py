import multiprocessing
import datetime as dt
import pytz

if __name__ == "__main__":
	import run 
	# p1 = multiprocessing.Process(target = run.start,args=(10,))
	# p1.start()
	# i = 0
	# while i < 100000000:
	# 	i = i +1
	# p1.terminate()
	# print("Terminated")
	started = False
	tz = pytz.timezone("Europe/Dublin")
	p1 = None
	while True:
		curr_time = dt.datetime.now(tz)
		if curr_time.hour ==13  and curr_time.minute == 30:
			# print(curr_time)
			if started == False:
				p1 = multiprocessing.Process(target = run.start,args=(10,))
				p1.start()
				started = True
				print("Started",curr_time)

		if curr_time.hour == 20:
			if started:
				p1.terminate()
				print("Terminated")
				print(curr_time)
				started = False
