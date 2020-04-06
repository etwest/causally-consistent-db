from flask import Flask, json, make_response, jsonify, render_template
from flask import request as flask_request
from flask_cors import CORS

from random import sample
from threading import Thread
import sqlite3 as SQL
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
import hashlib
import broadcast


app = Flask(__name__)
CORS(app) # allow everything through cross-origin-resource-sharing

# global variables
MB = 1000000
IP_PORT = os.getenv('IP_PORT')
VIEW = set(os.getenv('VIEW').split(','))
SHARD_COUNT = int(os.getenv('S'))

store = {}
# create an SQL database which we will use to store the data
# This database is stored within docker's volume so that it
# is persisted between restarts
# SQLconn = SQL.connect('keyValue-store_' + IP_PORT + '.data')

Shards = [[] for _ in range(SHARD_COUNT)]
Shard_Id = None

# Variable to control whether or not this node should be sending or accepting gossip requests
do_gossip = True

waiting = False

# Adding a node to a shard
# Will add the node to the shard with the fewest nodes
# ip_port is the new node to add to Shards
def addToShards(ip_port):
    minSize = sys.maxsize
    ind = 0
    pos = -1
    # Find the shard with the fewest nodes              
    for shard in Shards:
        if len(shard) < minSize:
            minSize = len(shard)
            pos = ind
        ind += 1
    Shards[pos].append(ip_port)

# 1. one shard has less than 2 nodes
# Function: removes a node specified by ip_port
def removeFromShards(ip_port):
    global SHARD_COUNT 
    # catches the case if there is 0 
    shard_id = -1
    for i, shard in enumerate(Shards):
        if ip_port in shard:
            shard_id = i

    assert shard_id != -1, "we couldn't find the shard_id"

    # If removing a shard will cause imbalance
    equalityCheck = len(Shards[shard_id]) < (len(VIEW) / SHARD_COUNT)
    twoCheck = len(Shards[shard_id]) == 2 # when we need to delete the entire shard
    # oneCheck = len(Shards[shard_id]) == 1 # only happens when there's one shard and one node

    Shards[shard_id].remove(ip_port)
    # removing first node to redistribute to current node
    # doesn't matter if it's last node or not
    if equalityCheck:
        # if there's an imbalance
        imax = 0 # holds index of max
        nmax = -1 # holds value of max
        for i, shard in enumerate(Shards):
            if len(shard) > nmax:
                nmax, imax = len(shard), i

        first_node = Shards[imax][0]                                
        Shards[imax].remove(first_node)
        Shards[shard_id].append(first_node)
        return True
    elif twoCheck:        
        first_node = Shards[shard_id].pop()
        Shards.pop(shard_id)

        imin = 0 # holds index of max
        nmin = sys.maxsize # holds value of max
        for i, shard in enumerate(Shards):
            if len(shard) < nmin:
                nmin, imin = len(shard), i

        Shards[imin].append(first_node)
        SHARD_COUNT -= 1
        return False

    return True

# Get count non-tombstone pairs in store
def len_shard():
    count = 0
    for _, entry in store.items():
        if entry.value:
            count += 1
    return count


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
        # gossip every 200 milliseconds
        while not self.stopped:
            if do_gossip:                        
                time.sleep(.2)                                       # sleep for 200 milliseconds
                temp_view = set(Shards[Shard_Id])
                if len(temp_view) > 1:
                    temp_view.remove(IP_PORT)                    # remove self from view temporarily
                    gossip_process_port = sample(temp_view, 1)[0]    # choose random process to gossip to
                    try:
                        requests.put('http://' + gossip_process_port + '/gossip', {"cur_store":store_to_JSON(), "sender": IP_PORT}, timeout=.5)
                    except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                        pass


class Entry:
    value = ""
    payload = {} # The Vector Clock for this variable
    timestamp = 0

    # This constructor takes a timestamp if given and creates a new Entry with the same timestamp
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

# Format our store properly so it can be sent with a message
def store_to_JSON():
    json_store = store.copy()
    for key, entry in json_store.items():
        time_str = str(entry.timestamp)
        json_dict = {'timestamp':time_str, 'value':entry.value, 'payload':entry.payload}
        json_store[key] = json_dict
    json_store = json.dumps(json_store)
    return json_store

# This function is used for creating a temp_store out of the store in json form given to us
# json_store is a json string which contains the a store (dictionary of key and entry)
def JSON_to_store(json_store):
    temp_store = {}
    json_store = json.loads(json_store)
    for key, value in json_store.items():
        value['timestamp'] = datetime.datetime.strptime(value['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
        temp_store[key] = Entry(value['value'], value['payload'], value['timestamp'])
    return temp_store

# Takes in two stores and sets the value to be the most recent
def compare_stores(other):
    if not other:
        other = {}
    else:
        other = json.loads(other)
    for key, value in other.items():
        if key not in store:
            value['timestamp'] = datetime.datetime.strptime(value['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            store[key] = Entry(value['value'], value['payload'], value['timestamp'])
        else:
            # Returns -1 if we won, 1 if they won
            value['timestamp'] = datetime.datetime.strptime(value['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            VC_compare = store[key].causal_compare(value)

            # If we won, do nothing.
            # If they won, copy the value into our store.
            if VC_compare == 1:
                store[key] = Entry(value['value'], value['payload'], value['timestamp'])


#####################################################################
###################       Server Resources      #####################
#####################################################################

# Fetches value of key
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
                response = make_response(jsonify({'result':"Error", 'error':'Key does not exist', 'payload': json.dumps(clientDict)}), 404)
                response.headers['Content-Type'] = 'application/json'
                return response
            elif store[key].compare_to(clientVC) <= 0:
                clientDict[key] = store[key].payload
                response = make_response(jsonify({'result': 'Success', 'value': store[key].value, 'owner': Shard_Id, 'payload': json.dumps(clientDict)}), 200)
                response.headers['Content-Type'] = 'application/json'
                return response
            else:
                response = make_response(jsonify({'result': 'Error', 'error': 'payload too old', 'payload': json.dumps(clientDict)}), 404)
                response.headers['Content-Type'] = 'application/json'
                return response
                
        # key doesn't exist, return error
        else:
            # this is the shard that's supposed to own it.
            if shard_hash(key) == Shard_Id:    
                response = make_response(jsonify({'result':"Error", 'error':'Key does not exist', 'payload': json.dumps(clientDict)}), 404)
                response.headers['Content-Type'] = 'application/json'
                return response

            # if key doesn't hash to this partition, forward the request.
            else:
                for node in Shards[shard_hash(key)]:
                    url = 'http://' + node + '/keyValue-store/' + key
                    try:
                        # return whatever response was returned.                        
                        r = requests.get(url, json.dumps(clientDict), timeout=.5)
                        return (r.content, r.status_code, r.headers.items())
                    except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                        continue

                # if all nodes in the partition are dead, return error
                response = make_response(jsonify({'result': 'Error', 'msg': 'Unable to access key ' + key, 'payload': json.dumps(clientDict)}), 400)
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
            response = make_response(jsonify({'result':'Success', 'isExists':True, 'owner': Shard_Id, 'payload': json.dumps(clientDict)}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            # check if this is the shard that's supposed to own it.
            if shard_hash(key) == Shard_Id:            
                response = make_response(jsonify({'result':'Success', 'isExists':False, 'payload': json.dumps(clientDict)}), 200)
                response.headers['Content-Type'] = 'application/json'
                return response

            # if key doesn't hash to this partition, forward the request.
            else:
                for node in Shards[shard_hash(key)]:
                    url = 'http://' + node + '/keyValue-store/search/' + key
                    try:
                        # return whatever response was returned.                        
                        r = requests.get(url, {'payload': json.dumps(clientDict)}, timeout=.5)
                        return (r.content, r.status_code, r.headers.items())
                    except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                        continue

                # if all nodes in the partition are dead, return error
                response = make_response(jsonify({'result': 'Error', 'msg': 'Unable to access key ' + key, 'payload': json.dumps(clientDict)}), 400)
                response.headers['Content-Type'] = 'application/json'
                return response

# insert or update key
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

        # if key doesn't hash to this partition, forward the request.
        if shard_hash(key) != Shard_Id:
            for node in Shards[shard_hash(key)]:
                url = 'http://' + node + '/keyValue-store/' + key
                try:
                    # forward return whatever response.                        
                    r = requests.put(url, {'payload': json.dumps(clientDict), 'val': value}, timeout=.5)
                    return (r.content, r.status_code, r.headers.items())
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    continue

            # if all the nodes in the partition are dead
            response = make_response(jsonify({'result': 'Error', 'msg': 'Unable to access key ' + key, 'payload': json.dumps(clientDict)}), 400)
            response.headers['Content-Type'] = 'application/json'
            return response

        # if empty value
        if not value:
            response = make_response(jsonify({'result':'Error', 'msg':"Value is missing", 'payload':json.dumps(clientDict)}), 422)
            response.headers['Content-Type'] = 'application/json'
            return response

        # key length is too long or too short; should be 1 <= len(key) <= 200
        if not 1 <= len(key) <= 200:
            response = make_response(jsonify({'result':'Error', 'msg':"Key not valid", 'payload':json.dumps(clientDict)}), 422)
            response.headers['Content-Type'] = 'application/json'
            return response

        # value is too big; should be 1mb max
        elif len(value) > MB:
            response = make_response(jsonify({'result':"Error", 'msg':'Object too large. Size limit is 1MB', 'payload':json.dumps(clientDict)}), 422)
            response.headers['Content-Type'] = 'application/json'
            return response

        # key already exists; update the value
        if (key in store) and (store[key].value != None):
            store[key].value = value
            store[key].timestamp = datetime.datetime.now()
            store[key].merge_VC(clientVC)
            clientDict[key] = store[key].payload
            response = make_response(jsonify({'replaced':True, 'msg':'Updated successfully', 'payload': json.dumps(clientDict)}), 201)
            response.headers['Content-Type'] = 'application/json'
            return response

        # key doesn't exist yet; add it
        else:
            # this is a VC
            entry_payload = {IP_PORT:0}

            store[key] = Entry(value, entry_payload)
            clientDict[key] = entry_payload

            response = make_response(jsonify({'replaced': False, 'msg': 'Added successfully', 'payload': json.dumps(clientDict)}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response


# deletes a key
@app.route('/keyValue-store/<key>', methods=['DELETE'])
def kvs_delete(key):
    if not waiting:
        clientVC = {}
        clientDict = {}
        payload = {}

        # if the payload is empty
        if flask_request.values.get('payload'):
            payload = flask_request.values.get('payload')
            clientDict = json.loads(payload)
            try:
                clientVC = clientDict[key]
            except:
                pass

        # if this key should be in a different partition
        if shard_hash(key) != Shard_Id:
            for node in Shards[shard_hash(key)]:
                url = 'http://' + node + '/keyValue-store/' + key
                try:
                    # forward return whatever response.                        
                    r = requests.delete(url, data={'payload': json.dumps(clientDict)}, timeout=.5)
                    return (r.content, r.status_code, r.headers.items())
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    continue
            
            # if all nodes in the partition are dead
            response = make_response(jsonify({'result': 'Error', 'msg': 'Unable to access key ' + key, 'payload': json.dumps(clientDict)}), 400)
            response.headers['Content-Type'] = 'application/json'
            return response

        # key already exists, delete it
        if key in store and store[key].value != None:
            store[key].value = None
            store[key].merge_VC(clientVC)
            clientDict[key] = store[key].payload
            response = make_response(jsonify({'result':'Success', 'msg':'Key deleted', 'payload':json.dumps(clientDict)}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            response = make_response(jsonify({'result':'Error', 'msg':'Key does not exist', 'payload':json.dumps(clientDict)}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response


#####################################################################
######################          Shard        ########################
#####################################################################


def shard_rebalance_store():
    # Create a list of dictionaries, each dictionary refers to the data that will be transferred to another Shard
    diff = [{} for _ in Shards]

    # make non-volatile copy of store
    temp = store.copy()
    
    # Loop through our store and figure out what stays and what moves    
    for key, entry in temp.items():
        # key_hash tells us where entry belongs
        key_hash = shard_hash(key)
        # if our key belongs to another shard, then add to diff
        # and delete
        if key_hash != Shard_Id:
            # place the key in the Shard that it now belongs in
            diff[key_hash][key] = entry
            # remove value from store
            del store[key]
    
    return diff

# endpoint to receive initial shard information
# from other nodes.
@app.route('/shard/init_receive', methods=['GET'])
def shard_init_receive():
    oth_ip_port = flask_request.values.get('ip_port')
    if oth_ip_port in VIEW:
        response = make_response(jsonify({'shards':Shards}), 200)
        response.headers['Content-Type'] = 'application/json'
        return response        
    else:
        response = make_response("not in my view", 400)
        response.headers['Content-Type'] = 'application/plain'
        return response

# When the number of shards in changed call this function on one node from each shard
# TODO: Should this function be called after a VIEW/Shard change or should it be called to propogate it itself?
#       currently acting as if this one is doing the propagation itself 
# This endpoint is used when the number of shards has either increased or decreased
# This change needs to be propogated in a very safe and specific manner
# Endpoint takes an argument of 'shards' which contians the new Shards list that it should adopt
@app.route('/shard/rebalance_primary', methods=['PUT'])
def shard_rebalance_primary():
    global do_gossip
    global Shards
    global SHARD_COUNT
    global Shard_Id
    do_gossip = False

    # Set internal view of shards to be what was given to us

    Shards = json.loads(flask_request.values.get('shards'))

    # get new shard id
    for i, shard in enumerate(Shards):
        if IP_PORT in shard:
            Shard_Id = i 
            break
            
    SHARD_COUNT = len(Shards)

    #Get the store of every node in my new shard
    for node in Shards[Shard_Id]:
        # pull the store out of r and perform a comparison/update of our store with the other store
        if node != IP_PORT:
            tries = 0
            while tries < 2:
                try:
                    r = requests.get('http://' + node + '/shard/rebalance_secondary', timeout=0.5)
                    oth_store = r.json()['store']
                    compare_stores(oth_store)
                    break
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    tries += 1
    
    #After I verify that I have everything from my shard friends... update membership of my store
    diff = shard_rebalance_store()
    # send the data that I am no longer responsible for to the Shards who are now responsible for it
    for i, shard in enumerate(diff):
        # exclude all empty lists (no need to send no data) and my own Shard
        if len(shard) > 0:
            tries = 0
            while tries < 2:
                try:
                    # convert shard to a json string
                    for key, entry in shard.items():
                        time_str = str(entry.timestamp)
                        json_dict = {'timestamp':time_str, 'value':entry.value, 'payload':entry.payload}
                        shard[key] = json_dict
                    shard = json.dumps(shard)

                    # send data
                    r = requests.put('http://' + Shards[i][1] + '/shard/updateStore', {'store': shard}, timeout=0.5)
                    break
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    tries += 1
    
    # After I update my store tell all the nodes in my shard to set their store to mine
    for node in Shards[Shard_Id]:
        if node != IP_PORT:
            tries = 0
            while tries < 2:
                try:
                    r = requests.put('http://' + node + '/shard/setStore', {'store':store_to_JSON(), 'shards': json.dumps(Shards)}, timeout=.5)
                    break
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    tries += 1
        
    # I'm finally done and can resume gossiping my data around
    do_gossip = True

    response = make_response("OK")
    response.headers['Content-Type'] = 'text/plain'
    return response

# the secondary nodes
@app.route('/shard/rebalance_secondary', methods=['GET'])
def shard_rebalance_secondary():
    global do_gossip
    do_gossip = False
    response = make_response(jsonify({'store':store_to_JSON()}),200)
    response.headers['Content-Type'] = 'application/json'
    return response

# used to override the internal store of a node with the new data
@app.route('/shard/setStore', methods=['PUT'])
def shard_setStore():
    # TODO: might need a json.loads here
    global Shards
    global SHARD_COUNT
    global do_gossip
    global Shard_Id
    global store

    newStore = flask_request.values.get('store')
    
    # Set internal view of shards to be what was given to us
    
    Shards = json.loads(flask_request.values.get('shards'))
    for i, shard in enumerate(Shards):
        if IP_PORT in shard:
            Shard_Id = i 
            break
    
    SHARD_COUNT = len(Shards)
    
    store = JSON_to_store(newStore)
    do_gossip = True
    response = make_response("OK",200)
    response.headers['Content-Type'] = 'application/text'
    return response

# used to merge the internal store of a node with new data
@app.route('/shard/updateStore', methods=['PUT'])
def shard_updateStore():
    newStore = flask_request.values.get('store')
    
    compare_stores(newStore)
    response = make_response("OK", 200)
    response.headers['Content-Type'] = 'application/text'
    return response

# get the id of the shard that this node belongs to
@app.route('/shard/my_id', methods=['GET'])
def shard_get_id():
    if not waiting:
        response = make_response(jsonify({'id': Shard_Id}), 200)            
        response.headers['Content-Type'] = 'application/json'
        return response

# get a list of all shard ids
@app.route('/shard/all_ids', methods=['GET'])
def shard_get_all():
    if not waiting:
        # return all ids
        ids = "0"
        for i in range(1, len(Shards)):
            ids += ","+str(i)          
        response = make_response(jsonify({'result': 'Success', 'shard_ids': ids}), 200)
        response.headers['Content-Type'] = 'application/json'
        return response        

# get the members of a shard
@app.route('/shard/members/<shard_id>', methods=['GET'])
def shard_get_members(shard_id):
    if not waiting:
        # invalid shard id
        shard_id = int(shard_id)
        if shard_id not in range(0,len(Shards)):
            response = make_response(jsonify({'result':'Error', 'msg': 'No shard with id ' + str(shard_id)}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response            
        
        members = ','.join(map(str, sorted(Shards[shard_id])))                                            
        response = make_response(jsonify({'result': 'Success', 'members': members}), 200)
        response.headers['Content-Type'] = 'application/json'
        return response

# get number of keys this shard is responsible for
@app.route('/shard/count/<shard_id>', methods=['GET'])
def shard_get_count(shard_id):
    if not waiting:
        # if invalid shard id
        shard_id = int(shard_id)
        if shard_id not in range(0,len(Shards)):
            response = make_response(jsonify({'result':'Error', 'msg': 'No shard with id ' + str(shard_id)}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response
        if shard_id == Shard_Id: #If requested shard ID is us
            response = make_response(jsonify({'result': 'Success', 'count': len_shard()}, 200))
            response.headers['Content-Type'] = 'application/json'
            return response
        else: # If requested shard ID is one of the other shards
            tries = 0
            while tries < 3:
                send_to = sample(Shards[shard_id], 1)[0]
                try:
                    # forward the request
                    r = requests.get('http://' + send_to + '/shard/count/'+str(shard_id), timeout=.5)
                    return (r.content, r.status_code, r.headers.items())
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    tries += 1

# change the number of shards we should have; calling this
# requires a rebalancing of nodes.
@app.route('/shard/changeShardNumber', methods=['PUT'])
def shard_change_num():
    if not waiting:
        global SHARD_COUNT
        global Shards
        global Shard_Id
        newshardNum = int(flask_request.values.get('num'))
        if newshardNum <= 0:
            response = make_response(jsonify({'result':'Error', 'msg': 'Cannot create 0 or negative shards'}), 400)
            response.headers['Content-Type'] = 'application/json'
            return response
        if newshardNum <= SHARD_COUNT:  
            # Then we are reducing the number of shards which won't cause errors
            # will need to do some rebalancing
            old_shards = []
            while len(Shards) > newshardNum:
                old_shards.append(Shards.pop()) 

            for shard in old_shards:
                for node in shard:
                    addToShards(node)     

            SHARD_COUNT = newshardNum
        elif newshardNum > SHARD_COUNT:
            # Check for errors
            if newshardNum > len(VIEW):
                response = make_response(jsonify({'result':'Error', 'msg':'Not enough nodes for '+str(newshardNum)+' shards'}), 400) 
                response.headers['Content-Type'] = 'application/json'
                return response
            # we should have at least 2 nodes per shard to be fault tolerant
            # doesn't make sense to have 0
            if len(VIEW) / newshardNum < 2: 
                response = make_response(jsonify({'result': 'Error', 'msg': 'Not enough nodes. ' + str(newshardNum) + ' shards result in a nonfault tolerant shard'}), 400) 
                response.headers['Content-Type'] = 'application/json'
                return response
            
            for i in range(newshardNum - SHARD_COUNT):
                Shards.append([])
            
            def existsSmallShard():
                minNodes = int(len(VIEW) / newshardNum)
                for shard in Shards:
                    if len(shard) < minNodes:
                        return True
                return False
                
            while (existsSmallShard()):
                moveFromHighestToLowest()     
            # If we've gotten to this point then it's time to redistribute the nodes/data
            # we want to take from the highest and give to the lowest until all nodes are equal
            SHARD_COUNT = newshardNum

        #update our Shard_Id
        for i, shard in enumerate(Shards):
            if IP_PORT in shard:
                Shard_Id = i
                
        # broadcast to everyone else 
        for shard in Shards:
            #if shard_count != Shard_Id:
            tries = 0
            while tries < 3:
                try:                 
                    requests.put('http://' + shard[0] + '/shard/rebalance_primary', {'shards': json.dumps(Shards)}, timeout=2)
                    break
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    tries += 1
        
        #rebalance our shard using us as primary
        # shard_rebalance_primary(json.dumps(Shards))

        ids = "0"
        for i in range(1, len(Shards)):
            ids += "," + str(i) 
        response = make_response(jsonify({'result':'Success', 'shard_ids': ids}), 200)
        response.headers['Content-Type'] = 'application/json'
        return response

def moveFromHighestToLowest():
    # pop node from highest length shard
    # put in lowest length shard the new node
    #print("moving from highest to lowest...")
    imax = 0 
    nmax = -1
    imin = 0
    nmin = sys.maxsize
    for i, shard in enumerate(Shards):
        if len(shard) < nmin:
            nmin=len(shard)
            imin=i        
        if len(shard) > nmax:
            nmax = len(shard)
            imax = i
    node = Shards[imax].pop(0)
    Shards[imin].append(node)


# hash a key to its shard
def shard_hash(value):
    m = hashlib.md5()
    m.update(value.encode())
    assert len(Shards) == SHARD_COUNT, "Error: length of shards not equal to shard count"
    return int(m.hexdigest(), 16) % len(Shards)

#####################################################################
###################          View Stuff         #####################
##################################################################### 

# gets VIEW
@app.route('/view', methods=['GET'])
def view_get():
    if not waiting:
        temp_view = VIEW.copy()
        response = make_response(jsonify({'view': ','.join(sorted(temp_view))}), 200)
        response.headers['Content-Type'] = 'application/json'
        return response

# adds new node to VIEW
@app.route('/view', methods=['PUT'])
def view_put():
    if not waiting:
        new_node = flask_request.values.get('ip_port')

        if new_node in VIEW:
            response = make_response(jsonify({'result':"Error", 'msg':str(new_node + ' is already in view')}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:

            # add this to shards array.
            VIEW.add(new_node)
            addToShards(new_node)

            # broadcast to every node except this one
            # send our new partitions array so that they can update as well
            for node in VIEW - { IP_PORT }:
                tries = 0
                while (tries < 3):
                    try:
                        # r = requests.put('http://' + node + '/view/update/ack', {'ip_port': new_node, 'pid': new_sid, 'shard_view': Shards}, timeout=0.5)
                        r = requests.put('http://' + node + '/view/update/ack', {'ip_port': new_node, 'shard_view': json.dumps(Shards)}, timeout=0.5)
                        if r.text == "OK":
                            break
                        else:
                            tries += 1
                    except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                        tries += 1
                        continue
            # urls = []
            # for node in VIEW - {IP_PORT}:
            #     urls.append(node + 'view/updata/ack')                
            # broadcast.put_broadcast(, .5)

            response = make_response(jsonify({'result':"Success", 'msg':str('Successfully added ' + new_node + ' to view')}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response


# deletes node from VIEW
@app.route('/view', methods=['DELETE'])
def view_delete():
    if not waiting:
        global Shards
        new_node = flask_request.values.get('ip_port')

        if new_node not in VIEW:
            response = make_response(jsonify({'result':"Error", 'msg':str(new_node + ' is not in current view')}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:

            # first removethe node from the Shrards
            # This function returns True if we should continue as normal
            # returns False if we should use the rebalancing protocol
            if removeFromShards(new_node):
                # broadcast to every node except this one
                for node in VIEW - { IP_PORT }:
                    tries = 0
                    while (tries < 3):
                        try:
                            r = requests.put('http://' + node + '/view/delete/ack', {'ip_port': new_node, 'shard_view': json.dumps(Shards)}, timeout=0.5)
                            if r.text == "OK":
                                break
                            else:
                                tries += 1
                        except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                            tries += 1
                            continue
            else:
                # This is called if the number of shards has been reduced by 1
                # removeFromShards already handled the movement of the nodes within the shard
                for shard in Shards:
                    tries = 0
                    while tries < 3:
                        try:                  
                            requests.put('http://' + shard[0] + '/shard/rebalance_primary', {'shards': json.dumps(Shards)}, timeout=4)
                            break
                        except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                            tries += 1
                            continue
                
                # broadcast the view change
                for node in VIEW - { IP_PORT }:
                    tries = 0
                    while (tries < 3):
                        try:
                            r = requests.put('http://' + node + '/view/delete/ack', {'ip_port': new_node, 'shard_view': json.dumps(Shards)}, timeout=0.5)
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
        # if this is the new node, set its shard id to be
        # the one assigned by the existing cluster.
        # if IP_PORT == new_view:
        #     Shard_Id = flask_request.values.get('pid')
        
        # should updating the shards be in this conditional?
        if new_view not in VIEW:
            VIEW.add(new_view)

        new_Shards = json.loads(flask_request.values.get('shard_view'))
        #print(new_Shards)
        if new_Shards:
            global Shards
            Shards = new_Shards
            #print('set Shards to', Shards)
        response = make_response("OK")
        response.headers['Content-Type'] = 'text/plain'
        return response

# ack view deletion request
@app.route('/view/delete/ack', methods=['PUT'])
def view_delete_ack():
    if not waiting:
        new_view = flask_request.values.get('ip_port')
        global Shards
        global store

        if new_view in VIEW:
            VIEW.remove(new_view)
        # check the new Shards to see if I was moved from one shard to another
        new_shard_list = json.loads(flask_request.values.get('shard_view'))

        for partition_num, partition in enumerate(Shards):
            if IP_PORT in partition:
                # If our shard number was changed which means we moved shards... delete our store
                if partition_num != Shard_Id:
                    store = {}
                break
        Shards = new_shard_list

        response = make_response("OK")
        response.headers['Content-Type'] = 'text/plain'
        # loop through shard partitions to find self
        return response


#####################################################################
######################        GOSSIP        #########################
#####################################################################

# secret endpoint to receive gossip requests
@app.route('/gossip', methods=['PUT'])
def gossip():
    if not waiting:
        if  do_gossip and flask_request.values.get('sender') in Shards[Shard_Id]:
            compare_stores(flask_request.values.get('cur_store'))
            response = make_response(jsonify({'result':"Gossip success"}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            # i'm a teapot
            response = make_response(jsonify({'result': "Either you're not real or I'm not gossiping"}, 418))
            response.headers['Content-Type'] = 'application/json'
            return response
    
#####################################################################
######################          HTML        #########################
#####################################################################
@app.route('/', methods=["GET"])
def home():
    return render_template("home.html", shards=SHARD_COUNT-1, nodes=len(VIEW))

#####################################################################
######################          MAIN        #########################
#####################################################################

def all_empty(new_shards):
    if not new_shards:
        return True
    for partition in new_shards:
        if len(partition) > 0:
            return False
    return True

if __name__ == "__main__":
    waiting = True
    while waiting:
        waiting = False
        empty = True
        for server in VIEW - {IP_PORT}:
            tries = 0
            while tries < 3:
                try:
                    r = requests.get('http://' + server + '/shard/init_receive', {"ip_port":IP_PORT}, timeout=2)
                    if r.status_code == 400:
                        waiting = True
                        empty = False
                        break
                    elif r.status_code == 200 and not all_empty(r.json()["shards"]):
                        empty = False 
                        #global Shards       
                        Shards = r.json()["shards"]
                        for i,shard in enumerate(Shards):
                            if IP_PORT in shard:
                                Shard_Id = i
                        break
                    tries += 1
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    # we assume that if we can't reach them that they aren't up
                    tries += 1
                    time.sleep(.2)
                    
        # do round robin
        if empty:
            temp_view = VIEW
            for port_count, port in enumerate(sorted(temp_view)):
                Shards[port_count % SHARD_COUNT].append(port)
                if port == IP_PORT:
                    Shard_Id = port_count % SHARD_COUNT
        
        # wait 200 milliseconds
        time.sleep(.2)

    g = Gossip_thread()
    #TODO: make init function that broadcasts to all other nodes,
    g.start()
    app.run(host="0.0.0.0", port=8080, threaded=True)
    g.stopped = True
    g.join()
    sys.exit(0)
