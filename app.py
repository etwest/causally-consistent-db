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
SHARD_COUNT = int(os.getenv('S'))

store = {}
Shards = [[] for _ in range(SHARD_COUNT)]
Shard_Id = None

#Variable to control whether or not this node should be sending or accepting gossip requests
do_gossip = True

waiting = False # will be set to false right after initial init

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

    #return pos

#TODO: check for imbalances
# 1. one shard has less than 2 nodes
# Function: removes a node specified by ip_port
def removeFromShards(ip_port):
    #shard_id = i 
    # catches the case if there is 0 
    for i, shard in enumerate(Shards):
        if ip_port in shard:
            shard_id = i
    # If removing a shard will cause imbalance
    equalityCheck = len(Shards[shard_id]) < (len(VIEW) / SHARD_COUNT)
    twoCheck = len(Shards[shard_id]) == 2 # when we need to delete the entire shard
    # oneCheck = len(Shards[shard_id]) == 1 # only happens when there's one shard and one node
    #now remove the 
    Shards[shard_id].remove(ip_port)
    # removing first node to redistribute to current node
    # doesn't matter if it's last node or not
    if equalityCheck:
        # if there's an imbalance
        imax = 0 # holds index of max
        nmax = -1 # holds value of max
        for i, shard in enumerate(Shards):
            if len(shard) > nmax:
                nmax = len(shard)
                imax = i
        first_node = Shards[imax][0]                                
        Shards[imax].remove(first_node)
        Shards[shard_id].append(first_node)
        return True
    # elif oneCheck:
    #     # Removes the last node from Shards
    #     first_node = Shards[shard_id][0]
    #     Shards[shard_id].remove(first_node)
    elif twoCheck:
        #TODO: remove ip port node and move other node
        #TODO: remove list from Shards                                                            
        first_node = Shards[shard_id][0]
        Shards[shard_id].remove(first_node)
        Shards.remove(shard_id)

        imin = 0 # holds index of max
        nmin = -1 # holds value of max
        for i, shard in enumerate(Shards):
            if len(shard) > nmin:
                nmin = len(shard)
                imin = i
        Shards[imin].append(first_node)
        return False
    return True


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
            if do_gossip:                        
                time.sleep(.2)                                       # sleep for 200 milliseconds
                #temp_view = VIEW.copy()
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

# This function is used for creating a temp_store out of the store in json form given to us
# json_store is a json string which contains the a store (dictionary of key and entry)
def JSON_to_store(json_store):
    temp_store = {}
    json_store = json.loads(json_store)
    for key, value in json_store.items():
        value['timestamp'] = datetime.datetime.strptime(value['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
        temp_store[key] = Entry(value['value'], value['payload'], value['timestamp'])
    return temp_store

def compare_stores(other):
    if not other:
        other = {}
    else:
        other = json.loads(other)
    for key, value in other.items():
        if key not in store:
            store[key] = Entry(value['value'], value['payload'])
        else:
            # returns -1 if we won, 1 if they won
            value['timestamp'] = datetime.datetime.strptime(value['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            VC_compare = store[key].causal_compare(value)

            # if we won, do nothing.
            # if they won, copy the value into our store.
            if VC_compare == 1:
                store[key] = Entry(value['value'], value['payload'], value['timestamp'])


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
                response = make_response(jsonify({'result': 'Success', 'value': store[key].value, 'owner': Shard_Id, 'payload': clientDict}), 200)
                response.headers['Content-Type'] = 'application/json'
                return response
            else:
                response = make_response(jsonify({'result': 'Error', 'error': 'payload too old', 'payload': clientDict}), 404)
                response.headers['Content-Type'] = 'application/json'
                return response
                
        # key doesn't exist, return error
        else:
            # this is the shard that's supposed to own it.
            if shard_hash(key) == Shard_Id:            
                response = make_response(jsonify({'result':"Error", 'error':'Key does not exist', 'payload': clientDict}), 404)
                response.headers['Content-Type'] = 'application/json'
                return response

            # if key doesn't hash to this partition, forward the request.
            else:
                for node in Shards[shard_hash(key)]:
                    url = 'http://' + node + '/keyValue-store/' + key
                    try:
                        # forward return whatever response.                        
                        r = requests.get(url, clientDict, timeout=.5)
                        return r
                    except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                        continue

                # if all nodes in the partition are dead
                response = make_response(jsonify({'result': 'Error', 'msg': 'Unable to access key ' + key, 'payload': clientDict}), 400)
                response.headers['Content-Type'] = 'application/json'
                return response

# Chercks whether the value exists
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
            # check if this is the shard that's supposed to own it.
            if shard_hash(key) == Shard_Id:            
                response = make_response(jsonify({'result':'Success', 'isExists':False, 'payload': clientDict}), 200)
                response.headers['Content-Type'] = 'application/json'
                return response

            # if key doesn't hash to this partition, forward the request.
            else:
                for node in Shards[shard_hash(key)]:
                    url = 'http://' + node + '/keyValue-store/search/' + key
                    try:
                        # forward return whatever response.                        
                        r = requests.get(url, {'payload': clientDict}, timeout=.5)
                        return r
                    except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                        continue

                # if all nodes in the partition are dead
                response = make_response(jsonify({'result': 'Error', 'msg': 'Unable to access key ' + key, 'payload': clientDict}), 400)
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

        # if key doesn't hash to this partition, forward the request.
        if shard_hash(key) != Shard_Id:
            for node in Shards[shard_hash(key)]:
                url = 'http://' + node + '/keyValue-store/' + key
                try:
                    # forward return whatever response.                        
                    r = requests.put(url, {'payload': clientDict, 'val': value}, timeout=.5)
                    return r
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    continue

            # if all the nodes in the partition are dead
            response = make_response(jsonify({'result': 'Error', 'msg': 'Unable to access key ' + key, 'payload': clientDict}), 400)
            response.headers['Content-Type'] = 'application/json'
            return response

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

            store[key] = Entry(value, entry_payload)
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
                    r = requests.delete(url, data={'payload': clientDict}, timeout=.5)
                    return r
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    continue
            
            # if all nodes in the partition are dead
            response = make_response(jsonify({'result': 'Error', 'msg': 'Unable to access key ' + key, 'payload': clientDict}), 400)
            response.headers['Content-Type'] = 'application/json'
            return response

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
def shard_update_store():
    # Create a list of dictionaries, each dictionary refers to the data that will be transferred another Shard
    diff = [{} for _ in Shards]
    #Loop through our store and figure out what stays and what moves
    for key, entry in store.items():
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
    do_gossip = False

    # Set internal view of shards to be what was given to us
    # TODO: json.loads?
    global Shards
    Shards = json.loads(flask_request.values.get('shards'))
    for i,shard in enumerate(Shards):
        if IP_PORT in shard:
            Shard_Id = i 
            break
    global SHARD_COUNT
    SHARD_COUNT = len(Shards)

    #Get the store of every node in my new shard
    for node in Shards[Shard_Id]:
        # pull the store out of r and perform a comparison/update of our store with the other store
        r = requests.get('http://' + node + '/shard/rebalance_secondary', timeout=.5)
        oth_store = r.json()['store']
        compare_stores(oth_store)
    
    #After I verify that I have everything from my shard friends... update membership of my store
    diff = shard_update_store()
   
    # send the data that I am no longer responsible for to the Shards who are now responsible for it
    for i, shard in enumerate(diff):
        # exclude all empty lists (no need to send no data) and my own Shard
        if len(shard) > 0:
            r = requests.put('http://' + Shards[i][1] + '/shard/updateStore', timeout=.5)
    
    #After I update my store tell all the nodes in my shard to set their store to mine
    for node in Shards[Shard_Id]:
        r = requests.put('http://' + node + '/shard/setStore', {'store':store_to_JSON(), 'shards': Shards}, timeout=.5)
    # I'm finally done and can resume gossiping my data around
    do_gossip = True

# the primary node
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
    
    #Set internal view of shards to be what was given to us
    
    Shards = flask_request.values.get('shards')
    for i,shard in enumerate(Shards):
        if IP_PORT in shard:
            Shard_Id = i 
            break
    
    SHARD_COUNT = len(Shards)
    
    #TODO: is this actually correct?
    store = JSON_to_store(newStore)
    do_gossip = True
    response = make_response("OK",200)
    response.headers['Content-Type'] = 'application/text'
    return response

# used to merge the internal store of a node with new data
@app.route('/shard/updateStore', methods=['PUT'])
def shard_updateStore():
    # TODO: might need a json.loads here
    newStore = flask_request.values.get('store')
    
    compare_stores(newStore)
    response = make_response("OK",200)
    response.headers['Content-Type'] = 'application/text'
    return response

@app.route('/shard/my_id', methods=['GET'])
def shard_get_id():
    if not waiting:
        response = make_response(jsonify({'id': Shard_Id}), 200)            
        response.headers['Content-Type'] = 'application/json'
        return response

@app.route('/shard/all_ids', methods=['GET'])
def shard_get_all():
    if not waiting:
        # return all id's
        ids = "0"
        for i in range(1, len(Shards)):
            ids += ","+str(i)          
        response = make_response(jsonify({'result': 'Success', 'shard_ids': ids}), 200)
        response.headers['Content-Type'] = 'application/json'
        return response        

@app.route('/shard/members/<shard_id>', methods=['GET'])
def shard_get_members(shard_id):
    if not waiting:
        # invalid shard id
        shard_id = int(shard_id)
        if shard_id not in range(0,len(Shards)):
            response = make_response(jsonify({'result':'Error', 'msg': 'No shard with id ' + str(shard_id)}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response            

        response = make_response(jsonify({'result': 'Success', 'members': ','.join(Shards[shard_id])}), 200)
        response.headers['Content-Type'] = 'application/json'
        return response

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
            response = make_response(jsonify({'result': 'Success', 'count': len(store)}, 200))
            response.headers['Content-Type'] = 'application/json'
            return response
        else: # If requested shard ID is one of the other shards
            tries = 0
            while tries < 2:
                send_to = sample(Shards[shard_id], 1)[0]
                try:
                    # forward the request
                    r = requests.get('http://' + send_to + '/shard/count/', timeout=.5)
                    response = make_response(r.text, r.status_code)
                    response.headers['Content-Type'] = 'application/json'
                    return response
                except(requests.HTTPError, requests.ConnectionError, requests.Timeout):
                    tries += 1

@app.route('/shard/changeShardNumber', methods=['PUT'])
def shard_change_num():
    if not waiting:
        shardNum = flask_request.values.get('num')
        if shardNum <= SHARD_COUNT:
            print ("nothing")
            #Then we are reducing the number of shards which won't cause errors
            #will need to do some rebalancing
            #TODO: destroy the last new_shardNum nodes and redistribute to
            # the beginnning shardNum nodes round robin style
#           new_shardNum = SHARD_COUNT - shardNum
#           SHARD_COUNT = 
#           for i in range(0,new_shardNum): # iterates through shards to be destroyed
#               for node in reversed(Shards[i]): # destroys and adds nodes to new lists
#                   Shards[i].pop(node)


        else:
            #Check for errors
            if shardNum > len(VIEW):
                response = make_response(jsonify({'result':'Error', 'msg':'Not enough nodes for '+str(shardNum)+'shards'}), 400) 
                response.headers['Content-Type'] = 'application/json'
                return response
            # we should have at least 2 nodes per shard to be fault tolerant
            # doesn't make sense to have 0
            if len(VIEW) / shardNum < 2: 
                response = make_response(jsonify({'result': 'Error', 'msg': 'Not enough nodes. ' + str(shardNum) + ' shards result in a nonfault tolerant shard'}), 400) 
                response.headers['Content-Type'] = 'application/json'
                return response
            #If we've gotten to this point then it's time to redistribute the nodes/data
    
    

# hash a key to its shard
def shard_hash(value):
    return hash(value) % len(Shards)        

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

            # first, add the new node to our view.
            # then, find the partition with least number of nodes
            # and add the node to that.

            response = make_response(jsonify({'result':"Success", 'msg':str('Successfully added ' + new_node + ' to view')}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response


# deletes node from VIEW
@app.route('/view', methods=['DELETE'])
def view_delete():
    if not waiting:
        new_node = flask_request.values.get('ip_port')

        if new_node not in VIEW:
            response = make_response(jsonify({'result':"Error", 'msg':str(new_node + ' is not in current view')}), 404)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            # need to be careful. what happens if the node
            # being deleted still thinks that it's in the view?

            # first removethe node from the Shrards
            # This function returns True if we should continue as normal
            # returns False if we should use the rebalancing protocol
            removeFromShards(new_node)

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

            VIEW.remove(new_node)
            #removeFromShards(new_node)

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
        print(new_Shards)
        if new_Shards:
            global Shards
            Shards = new_Shards
            print('set Shards to', Shards)
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
            Shards = json.loads(flask_request.values.get('shard_view'))
        # check the new Shards to see if I was moved from one shard to another
        for partition_num, partition in enumerate(Shards):
            if IP_PORT in partition:
                # found ourselves in partition
                if partition_num != Shard_Id:
                    store = {}
                break
        new_shard_list = json.loads(flask_request.values.get('shards'))
        if not new_shard_list:
            print("we fucked up lmaooo")
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
    if not waiting and do_gossip:
        if flask_request.values.get('sender') in Shards[Shard_Id]:
            compare_stores(flask_request.values.get('cur_store'))
            response = make_response(jsonify({'result':"Gossip success"}), 200)
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            # i'm a teapot
            response = make_response(jsonify({'result': "You're not real or I am not gossiping"}, 418))
            response.headers['Content-Type'] = 'application/json'
            return response


#####################################################################
######################          MAIN        #########################
#####################################################################

def all_empty(new_shards):
    if not new_shards:
        print("We fucked up again lmaoooo\n")
        return True
    for partition in new_shards:
        if len(partition) > 0:
            return False
    return True

if __name__ == "__main__":
    waiting = True
    while waiting:
        print('looping init\n')
        waiting = False
        empty = True
        for server in VIEW - {IP_PORT}:     
            tries = 0
            while tries < 3:
                try:
                    r = requests.get('http://' + server + '/shard/init_receive', {"ip_port":IP_PORT}, timeout=2)
                    print("requesting", server)
                    print(r.status_code)
                    print(r.text)
                    if r.status_code == 400:
                        print('blocked on a server\n')
                        waiting = True
                        empty = False
                        break
                    elif r.status_code == 200 and not all_empty(r.json()["shards"]):
                        # TODO: eugene rewrites the his code that baiwen deleted xdddd
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
                    print(Shard_Id)
        
        # wait 200 milliseconds
        time.sleep(.2)

    
    print(Shards)
    print(Shard_Id)
    g = Gossip_thread()
    #TODO: make init function that broadcasts to all other nodes,
    g.start()
    app.run(host="0.0.0.0", port=8080, threaded=True)
    g.stopped = True
    g.join()
    sys.exit(0)
