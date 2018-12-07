import requests
import asyncio
import concurrent.futures
import time

def send_get_broadcast(urls):
	answer = []
	size = len(urls)
	async def broadcast():
		with concurrent.futures.ThreadPoolExecutor(max_workers=size%30) as executor:
			loop = asyncio.get_event_loop()
			futures = [0 for _ in range(size)]
			for i in range(size):
				print(urls[i])
				futures[i] = loop.run_in_executor(executor, requests.get, urls[i], timeout=.5)
			for response in await asyncio.gather(*futures):
				answer.append(response)	
	
	loop = asyncio.get_event_loop()
	loop.run_until_complete(broadcast())
	return answer

def send_put_broadcast(urls):
	answer = []
	size = len(urls)
	async def broadcast():
		with concurrent.futures.ThreadPoolExecutor(max_workers=size%30) as executor:
			loop = asyncio.get_event_loop()
			futures = [0 for _ in range(size)]
			for i in range(size):
				print(urls[i])
				futures[i] = loop.run_in_executor(executor, requests.put, urls[i], timeout=.5)
			for response in await asyncio.gather(*futures):
				answer.append(response)	
	
	loop = asyncio.get_event_loop()
	loop.run_until_complete(broadcast())
	return answer