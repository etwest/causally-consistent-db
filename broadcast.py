import requests
import time
from threading import Thread

class Get_thread(Thread):
	def __init__(self, url, timeout, payload):
		Thread.__init__(self)
		self.result = None
		self.failed = False
		self.daemon = True
		self.url = url
		self.timeout = timeout
		self.payload = payload

	def run(self):
		tries = 0
		r = None
		while tries < 3:
			try:
				if self.payload != None:
					r = requests.get(self.url, self.payload, timeout=self.timeout)
				else:
					r = requests.get(self.url, timeout=self.timeout)
				self.result = r 
				return
			except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
				tries += 1
		self.failed = True

class Put_thread(Thread):
	def __init__(self, url, timeout, payload):
		Thread.__init__(self)
		self.result = None
		self.failed = False
		self.daemon = True
		self.url = url
		self.timeout = timeout
		self.payload = payload

	def run(self):
		tries = 0
		r = None
		while tries < 3:
			try:
				if self.payload != None:
					r = requests.put(self.url, self.payload, timeout=self.timeout)
				else:
					r = requests.put(self.url, timeout=self.timeout)
				self.result = r
				return
			except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
				tries += 1
		self.failed = True

#####################################################
############# Functions to actually use #############
#####################################################

# urls is a list of the urls to query and timeout is a float number
#	which is hold long to wait for a response in seconds before retrying
#	payload holds a dictionary to send to the endpoint

# returns a list of either False (if the request failed) or a response object
def get_broadcast(urls, timeout, payload=None):
	answer = []
	threads = [
		Get_thread(url, timeout, payload) for url in urls
	]
	for i in range(len(urls)):
		threads[i].start()
	for i in range(len(urls)):
		threads[i].join()
		if not threads[i].failed:
			answer.append(threads[i].result)
		else:
			answer.append(False)
	return answer

# urls is a list of the urls to query and timeout is a float number 
#	which is hold long to wait for a response in seconds before retrying
#	payload holds a dictionary to send to the endpoint

# returns a list of either False (if the request failed) or a response object
def put_broadcast(urls, timeout, payload=None):
	answer = []
	threads = [
		Put_thread(url, timeout, payload) for url in urls
	]
	for i in range(len(urls)):
		threads[i].start()
	for i in range(len(urls)):
		threads[i].join()
		if not threads[i].failed:
			answer.append(threads[i].result)
		else:
			answer.append(False)
	return answer


# Example of using these functions to send a broadcast of 20 messages to example.com (GET and PUT)

# urls = ['http://192.123.123.12/'] #a url that should fail to connect
# for _ in range(20):
# 	urls.append('http://www.example.com/')
# curr = time.time()
# responses = get_broadcast(urls, .5)
# print(time.time() - curr)
# for r in responses:
# 	if r != False:
# 		print(r.text)
# 	else:
# 		print(False)

# curr = time.time()
# responses = put_broadcast(['http://www.example.com' for _ in range(20)], .5)
# print(time.time() - curr)
# for r in responses:
# 	if r != False:
# 		print(r.text)
# 	else:
# 		print(False)