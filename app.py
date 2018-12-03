from flask import Flask, json, make_response, jsonify
from flask import request as flask_request
from random import sample
from threading import Thread
import json
import os
import requests
import time
import datetime
import signal
import sys
import copy
import threading
import collections

app = Flask(__name__)

# global variables
MB = 1000000
IP_PORT = os.getenv('IP_PORT')
VIEW = set(os.getenv('VIEW').split(','))
SHARD_COUNT = os.getenv('S')

store = {}
Shards = [[] for _ in range(SHARD_COUNT)]
Shard_Id = None


#####################################################################
###################           Classes           #####################
##################################################################### 

class Gossip_thread(Thread):
    stopped = False

    def __init__(self):
        Thread.__init__(self)
        self.daemon = True

    # TODO: Also pass IP_PORT with gossip, so can check if in view

    def run(self):
        # gossip every 125 milliseconds
        while not self.stopped:
            time.sleep(.2)                                       # sleep for 200 milliseconds
            #temp_view = VIEW.copy()
            temp_view = set(Shards[Shard_Id])
            if len(temp_view) > 1:
                temp_view.remove(IP_PORT)                    # remove self from view temporarily
                gossip_process_port = sample(temp_view, 1)[0]    # choose random process to gossip to
                try:
                    r = requests.put('http://' + gossip_process_port + '/gossip', {"cur_store":store_to_JSON(), "sender": IP_PORT}, timeout=.5)
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    pass


class Entry:
    value = ""
    payload = {} # The Vector Clock for this variable
    timestamp = 0

    # This constructor takes a timestamp if given and creates a new entry with the same timestamp
    # otherwise sets timestamp to datetime
    def __init__(self, value, payload, timestamp=datetime.datetime.now()):
        self.timestamp = timestamp
        self.value = value
        self.payload = payload

    # Helper: takes 2 dictionaries
    # used for vector clocks
    # returns firstGreater=True if dict1 is greater
    # returns secGreater=True if dict2 is greater
    def dict_compare_to(self, dict1, dict2):
        firstGreater = True
        secGreater = True
        for key, _ in dict1.items():
            if key in dict2:
                if dict1[key] < dict2[key]:
                    firstGreater = False
            else:
                secGreater = False
        return firstGreater, secGreater
    # Class Function: Takes takes 1 payload
    # returns 0 if payload is equal to current payload
    # returns 1 if payload is newer
    # returns -1 if payload is older                                                     

    def compare_to(self, new_payload):
        current_to_new = self.dict_compare_to(self.payload,new_payload)
        new_to_current = self.dict_compare_to(new_payload,self.payload)

        if current_to_new[0] and current_to_new[1] and new_to_current[0] and new_to_current[1]: # incomparable
            return 0
        elif current_to_new[0] and new_to_current[1]:
            return -1
        elif current_to_new[1] and new_to_current[0]:
            return 1
        else:
            return 0

    # function to merge two vector clocks
    def merge_VC(self, entry2):
        for key, _ in entry2.items():
            if key not in self.payload:
                self.payload[key] = entry2[key]
            else:
                self.payload[key] = max(self.payload[key], entry2[key])
        if IP_PORT in self.payload:
            self.payload[IP_PORT] += 1
        else:
            self.payload[IP_PORT] = 0

    # Function for causally comparing two entries
    # This function assumes that we will get a dict as the second entry in form
    # {"value": val, "timestamp": time, "payload": VC}
    def causal_compare(self, otherDict):
        order = self.compare_to(otherDict["payload"])
        if order == -1: # we win
            return -1
        if order == 1: # they win
            return 1
        # Otherwise compare the timestamps
        oth_time = otherDict["timestamp"]
        if oth_time < self.timestamp:
            return -1 # our timestamp greater so we win
        return 1 # they win                            


def store_to_JSON():
    json_store = store.copy()
    for key, entry in json_store.items():
        time_str = str(entry.timestamp)
        json_dict = {'timestamp':time_str, 'value':entry.value, 'payload':entry.payload}
        json_store[key] = json_dict
    json_store = json.dumps(json_store)
    return json_store


def compare_stores(other):
    if not other:
        other = {}
    else:
        other = json.loads(other)
    for key, value in other.items():
        if key not in store:
            store[key] = entry(value['value'], value['payload'])
        else:
            # returns -1 if we won, 1 if they won
            value['timestamp'] = datetime.datetime.strptime(value['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            VC_compare = store[key].causal_compare(value)

            # if we won, do nothing.
            # if they won, copy the value into our store.
            if VC_compare == 1:
                store[key] = entry(value['value'], value['payload'], value['timestamp'])


#####################################################################
###################       Server Resources      #####################
#####################################################################

# fetches value of key
@app.route('/keyValue-store/<key>', methods=['GET'])
def kvs_get(key):
    if not waiting:
        clientVC = {}
        clientDict = {}
        payload = ""

        # This code is for extracting the minimum version of the client's request
        if flask_request.values.get('payload'):
            payload = flask_request.values.get('payload')
            clientDict = json.loads(payload)
            try:
                clientVC = clientDict[key]
            except:
                pass

        # key exists, return the value
        if key in store:
            #TODO: compare the client VC to the server VC
            if store[key].value == None:
                response = make_response(jsonify({'result':"Error", 'error':'Key does not exist', 'payload': clientDict}), 404)
                response.headers['Content-Type'] = 'application/json'
                return response
            elif store[key].compare_to(clientVC) <= 0:
                clientDict[key] = store[key].payload
                response = make_response(jsonify({'result': 'Success', 'value': store[key].value, 'payload': clientDict}), 200)
                response.headers['Content-Type'] = 'application/json'
                return response
            else:
                response = make_response(jsonify({'result': 'Error', 'error': 'payload too old', 'payload': clientDict}), 404)
                response.headers['Content-Type'] = 'application/json'
                return response
        # key doesn't exist, return error
        else:
            response = make_response(jsonify({'result':"Error", 'error':'Key does not exist', 'payload': clientDict}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response


# Checks whether the value exists
@app.route('/keyValue-store/search/<key>', methods=['GET'])
def kvs_search(key):
    if not waiting:
        clientDict = {}
        payload = {}
        if flask_request.values.get('payload'):
            payload = flask_request.values.get('payload')
            clientDict = json.loads(payload)
        # if the key exists, return true, otherwise return false
        if key in store and store[key].value != None:
            response = make_response(jsonify({'result':'Success', 'isExists':True, 'payload':clientDict}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            response = make_response(jsonify({'result':'Success', 'isExists':False, 'payload': clientDict}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response


@app.route('/keyValue-store/<key>', methods=['PUT'])
def kvs_put(key):
    if not waiting:
        clientVC = {}
        clientDict = {}
        payload = {}

        # This code is for extracting the minimum version of the client's request
        if flask_request.values.get('payload'):
            payload = flask_request.values.get('payload')
            clientDict = json.loads(payload)
            try:
                clientVC = clientDict[key]
            except:
                pass

        value = flask_request.values.get('val')

        # if empty payload
        if not value:
            response = make_response(jsonify({'result':'Error', 'msg':"Value is missing", 'payload':clientDict}), 422)
            response.headers['Content-Type'] = 'application/json'
            return response

        # key length is too long or too short; should be 1 <= len(key) <= 200
        if not 1 <= len(key) <= 200:
            response = make_response(jsonify({'result':'Error', 'msg':"Key not valid", 'payload':clientDict}), 422)
            response.headers['Content-Type'] = 'application/json'
            return response

        # value is too big; should be 1mb max
        elif len(value) > MB:
            response = make_response(jsonify({'result':"Error", 'msg':'Object too large. Size limit is 1MB', 'payload':clientDict}), 422)
            response.headers['Content-Type'] = 'application/json'
            return response

        # key already exists; update the value
        if (key in store) and (store[key].value != None):
            store[key].value = value
            store[key].timestamp = datetime.datetime.now()
            store[key].merge_VC(clientVC)
            clientDict[key] = store[key].payload
            response = make_response(jsonify({'replaced':True, 'msg':'Updated successfully', 'payload': clientDict}), 201)
            response.headers['Content-Type'] = 'application/json'
            return response

        # key doesn't exist yet; add it
        else:
            # this is a VC
            entry_payload = {IP_PORT:0}

            store[key] = entry(value, entry_payload)
            clientDict[key] = entry_payload

            response = make_response(jsonify({'replaced': False, 'msg': 'Added successfully', 'payload': clientDict}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response

@app.route('/keyValue-store/<key>', methods=['DELETE'])
def kvs_delete(key):
    if not waiting:
        clientVC = {}
        clientDict = {}
        payload = {}

        if flask_request.values.get('payload'):
            payload = flask_request.values.get('payload')
            clientDict = json.loads(payload)
            try:
                clientVC = clientDict[key]
            except:
                pass

        # key already exists, delete it
        if key in store and store[key].value != None:
            store[key].value = None
            store[key].merge_VC(clientVC)
            clientDict[key] = store[key].payload
            response = make_response(jsonify({'result':'Success', 'msg':'Key deleted', 'payload':clientDict}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            response = make_response(jsonify({'result':'Error', 'msg':'Key does not exist', 'payload':clientDict}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response


#####################################################################
######################          Shard        ########################
##################################################################### 
@app.route('/shard/init_receive', methods=['GET'])
def shard_init_receive():
    payload = flask_request.values.get('ip_port')
    if payload in VIEW:
        response = make_response(jsonify('shards':Shards), 200)
        response.headers['Content-Type'] = 'application/json'
        return response        
    else:
        response = make_respose(400)
        response.headers['Content-Type'] = 'application/json'
        return response

@app.route('/shard/my_id', methods=['GET'])
def shard_get_id():
    if not waiting:
        response = make_response(jsonify('id': Shard_Id), 200)            
        response.headers['Content-Type'] = 'application/json'
        return response


@app.route('/shard/all_ids', methods=['GET'])
def shard_get_all():
    if not waiting:
    # return all id's
        ids = "0"
        for i in range(1, len(Shards)):
            ids += ","+str(i)                


@app.route('/shard/members/<shard_id>', methods=['GET'])
def shard_get_members(shard_id):
    if not waiting:
        # invalid shard id
        if shard_id >= len(Shards) or shard_id < 0:
            response = make_response(jsonify('result':'Error', 'msg': 'No shard with id ' + str(shard_id)), 404)
            response.headers['Content-Type'] = 'application/json'
            return response            

        response = make_response(jsonify('result': 'Success', 'members': (',').join(Shards[shard_id])), 200)
        response.headers['Content-Type'] = 'application/json'
        return response
            
@app.route('shard/changeShardNumber', methods=['PUT'])
def shard_change_num():
    if not waiting:
        shardNum = flask_request.values.get('num')
        # assuming shardNum is int
        response.headers['Content-Type'] = 'application/json'
        return response            

@app.route('/shard/count/<shard_id>', methods=['GET'])
def shard_get_count(shard_id):
    if not waiting:
        if shardNum <= SHARD_COUNT:
            #Then we are reducing the number of shards which won't cause errors
            #will need to do some rebalancing
        else:
            #Check for errors
            if shardNum > len(VIEW):
                response = make_response(jsonify('result':'Error', 'msg':'Not enough nodes for '+str(shardNum)+'shards'), 400)                     
                response.headers['Content-Type'] = 'application/json'
                return response
            # we should have at least 2 nodes per shard to be fault tolerant
            # doesn't make sense to have 0
            if len(VIEW)/shardNum < 2: 
                response = make_response(jsonify('result': 'Error', 'msg': 'Not enough nodes. ' + str(shardNum) + ' shards result in a nonfault tolerant shard',400) 
                response.headers['Content-Type'] = 'application/json'
                return response
            #If we've gotten to this point then it's time to redistribute the nodes/data
                              
# hash a key to its shard
def shard_hash(value):
    return hasn(value) % len(Shards)        

#####################################################################
###################          View Stuff         #####################
##################################################################### 

@app.route('/view', methods=['GET'])
def view_get():
    if not waiting:
        temp_view = VIEW.copy()
        response = make_response(jsonify({'view': ','.join(sorted(temp_view))}), 200)
        response.headers['Content-Type'] = 'application/json'
        return response


@app.route('/view', methods=['PUT'])
def view_put():
    if not waiting:
        new_node = flask_request.values.get('ip_port')

        if new_node in VIEW:
            response = make_response(jsonify({'result':"Error", 'msg':str(new_node + ' is already in view')}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            VIEW.add(new_node)
            # broadcast to every node except this one
            for node in VIEW - { IP_PORT }:
                tries = 0
                while (tries < 3):
                    try:
                        r = requests.put('http://' + node + '/view/update/ack', {'ip_port': new_node }, timeout=0.5)
                        if r.text == "OK":
                            break
                        else:
                            tries += 1
                    except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                        tries += 1
                        continue
            response = make_response(jsonify({'result':"Success", 'msg':str('Successfully added ' + new_node + ' to view')}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response


@app.route('/view', methods=['DELETE'])
def view_delete():
    if not waiting:
        new_node = flask_request.values.get('ip_port')

        if new_node not in VIEW:
            response = make_response(jsonify({'result':"Error", 'msg':str(new_node + ' is not in current view')}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            # broadcast to every node except this one
            for node in VIEW - { IP_PORT }:
                tries = 0
                while (tries < 3):
                    try:
                        r = requests.put('http://' + node + '/view/delete/ack', {'ip_port': new_node }, timeout=0.5)
                        if r.text == "OK":
                            break
                        else:
                            tries += 1
                    except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                        tries += 1
                        continue

            VIEW.remove(new_node)
            response = make_response(jsonify({'result':"Success", 'msg':str('Successfully removed ' + new_node + ' from view')}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response


#####################################################################
######################          ACK         #########################
#####################################################################

# ack view update request
@app.route('/view/update/ack', methods=['PUT'])
def view_update_ack():
    if not waiting:
        new_view = flask_request.values.get('ip_port')
        if new_view not in VIEW:
            VIEW.add(new_view)
        response = make_response("OK")
        response.headers['Content-Type'] = 'text/plain'
        return response

# ack view deletion request
@app.route('/view/delete/ack', methods=['PUT'])
def view_delete_ack():
    if not waiting:
        new_view = flask_request.values.get('ip_port')
        if new_view in VIEW:
            VIEW.remove(new_view)
        response = make_response("OK")
        response.headers['Content-Type'] = 'text/plain'
        return response


#####################################################################
######################        GOSSIP        #########################
#####################################################################

# secret endpoint to receive gossip requests
@app.route('/gossip', methods=['PUT'])
def gossip():
    if not waiting:
        if flask_request.values.get('sender') in VIEW:
            compare_stores(flask_request.values.get('cur_store'))
            response = make_response(jsonify({'result':"Gossip success"}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            # i'm a teapot                                    
            response = make_response(jsonify({'result': "You're not real"}, 418))
            response.headers['Content-Type'] = 'application/json'
            return response                                


#####################################################################
######################          MAIN        #########################
#####################################################################

if __name__ == "__main__":
    for server in VIEW:
        r = requests.put('http://' + server + '/shard/init_receive', {"ip_port":IP_PORT}, timeout=2)
        if r.status_code == 400:
            waiting = True
        elif r.status_code == 200 and Shards:
    g = gossip_thread()
    #TODO: make init function that broadcasts to all other nodes,
    g.start()
    app.run(host="0.0.0.0", port=8080, threaded=True)
    g.stopped = True
    g.join()
    sys.exit(0)
