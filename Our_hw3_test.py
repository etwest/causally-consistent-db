#!/usr/bin/env python

"""
This is a testing script for Vector Clocks, currently only deals with a single server
Requires a clean build of the server with no prior writes
"""

import requests
import json
import time

servers = ["localhost:8080", "localhost:8081", "localhost:8082", "localhost:8083", "localhost:8084", "localhost:8085"]
VIEW = ["10.0.0.10:8080", "10.0.0.11:8080", "10.0.0.12:8080", "10.0.0.13:8080", "10.0.0.14:8080", "10.0.0.15:8080"]

# =========================== Test against 8080 ===================================
params = {"val":"test_value"}
r = requests.put("http://" + servers[0] + "/keyValue-store/test", params=params)
expected = {"msg":"Added successfully","payload":"{\"test\":{\"10.0.0.10:8080\":0}}","replaced":False}
payload = json.loads(r.json()['payload'])

if (payload == {'test':{'10.0.0.10:8080':0}} and r.json()['msg'] == 'Added successfully' and r.json()['replaced'] == False):
    print("test PUT 1 passed")
else:
    print(__doc__)
    print("test PUT 1 FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)

r = requests.get("http://"+ servers[0] +"/keyValue-store/test")
expected = {"payload":"{\"test\":{\"10.0.0.10:8080\":0}}","result":"Success","value":"test_value","owner":'0'}
# json = json.loads(r.text)
data = r.json()
client_payload = data['payload']
if json.loads(client_payload) == {"test":{"10.0.0.10:8080":0}} and data['result'] == "Success" and data['value'] == "test_value" and data['owner'] == 0:
    print("test GET 1 PASSED")
else:
    print("test GET 1 FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)

# Test delete function
r = requests.delete("http://" + servers[0] + "/keyValue-store/test")
expected = {"payload":{"test":{"10.0.0.10:8080":1}},"result":"Success","msg":"Key deleted"}
data = r.json()
client_payload = data['payload']
if (json.loads(client_payload) == {"test":{"10.0.0.10:8080":1}}) and (data['result'] == 'Success') and (data['msg'] == 'Key deleted'):
    print("test DELETE 1 passed")
else:
    print("test DELETE 1 FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)
# Test that we can no longer read deleted
r = requests.get("http://" + servers[0] + "/keyValue-store/test", {'payload': client_payload})
expected = {'result':"Error", 'error':'Key does not exist', 'payload': client_payload}
data = r.json()
client_payload = data['payload']
if data['result'] == 'Error' and data['error'] == 'Key does not exist':
    print("test GET 2 passed")
else:
    print("test GET 2 FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)

# Test that we can now put back to deleted
params = {"val":"test_value"}
r = requests.put("http://" + servers[0] + "/keyValue-store/test", params=params)
# expected = {"msg":"Added successfully","payload":{"test":{"10.0.0.10:8080":0}},"replaced":"False"}
data = r.json()
client_payload = data['payload']
if json.loads(client_payload) == {"test":{"10.0.0.10:8080":0}} and data['msg'] == "Added successfully" and data['replaced'] == False:
    print("test PUT 2 passed")
else:
    print("test PUT 2 FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)

# Test that we can get new value
r = requests.get("http://" + servers[0] + "/keyValue-store/test", {'payload': client_payload})
expected = {"payload":{"test":{"10.0.0.10:8080":0}},"result":"Success","value":"test_value"}
data = r.json()
client_payload = data['payload']
if json.loads(client_payload) == {"test":{"10.0.0.10:8080":0}} and data['result'] == 'Success' and data['value'] == 'test_value':
    print("test GET 3 passed")
else:
    print("test GET 3 FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)

# Test that Vector Clocks properly block us
client_payload = "{\"test\":{\"10.0.0.10:8080\":5}}"
r = requests.get("http://" + servers[0] + "/keyValue-store/test", {'payload': client_payload})
expected = {"payload":{"test":{"10.0.0.10:8080":5}},"result":"Error","error":"payload too old"}
data = r.json()
client_payload = data['payload']
if json.loads(client_payload) == {"test":{"10.0.0.10:8080":5}} and data['result'] == 'Error' and data['error'] == 'payload too old':
    print("test GET 4 passed")
else:
    print("test GET 4 FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)


# TODO: search for existing key
server = servers[0]
key = "test"
client_payload = "{\"test\":{\"10.0.0.10:8080\":0}}"
r = requests.get("http://%s/keyValue-store/search/%s" % (server, key), params={'payload':client_payload})
expected = {"isExists":'true', "result":"Success", "payload":{"test":{"10.0.0.10:8080":0}}}
expected_status_code = 200
data = r.json()
if json.loads(data['payload']) == {"test":{"10.0.0.10:8080":0}} and data['isExists'] == True and data['result'] == "Success":
    print("search for existing key test: PASSED")
elif r.status_code != expected_status_code:
    print("search for existing key test status code: FAILED")
else:
    print("search for existing key test: FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)

# TODO: search for non-existent key
server = servers[0]
key = "not_in_store"
client_payload = "{\"test\":{\"10.0.0.10:8080\":0}}"
r = requests.get("http://%s/keyValue-store/search/%s" % (server, key), params={'payload':client_payload})
expected = {"isExists":False, "result":"Success", "payload":{"test":{"10.0.0.10:8080":0}}}
expected_status_code = 200
data = r.json()
if json.loads(data['payload']) == {"test":{"10.0.0.10:8080":0}} and data['isExists'] == False and data['result'] == "Success":
    print("search for non-existent key test: PASSED")
elif r.status_code != expected_status_code:
    print("search for non-existent key test status code: FAILED")
else:
    print("search for non-existent key test: FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)

# test create a new node 
#   have build script with a 6th container that is not in the view which we add in
#   Then check that it gets all the keys
# Insert another key value pair to the first node
r = requests.put("http://localhost:8080/keyValue-store/test2", params={'val':'test_value2'})

# Put a value into the container that is not a part of the current view
r = requests.put("http://localhost:8085/keyValue-store/test3", params={'val':'test_value3'})
# Add a new container to the view
r = requests.put("http://" + servers[0] + "/view", params={'ip_port': '10.0.0.15:8080'})
for node in VIEW:
    r = requests.put("http://localhost:8085/view", params={'ip_port': node})
time.sleep(2.5)
#Now test that the new container has the right values
r = requests.get("http://localhost:8085/keyValue-store/test")
expected = {"payload":{"test":{"10.0.0.10:8080":0}},"result":"Success","value":"test_value"}
data = r.json()
if json.loads(data['payload']) == {"test":{"10.0.0.10:8080":0}} and data['owner'] == 0 and data['result'] == "Success" and data['value'] == 'test_value':
    print("New node 1 PASSED")
else:
    print("New node 1 FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)
r = requests.get("http://localhost:8085/keyValue-store/test2")
expected = {"payload":{"test2":{"10.0.0.10:8080":0}},"result":"Success","value":"test_value2"}
data = r.json()
if json.loads(data['payload']) == {"test2":{"10.0.0.10:8080":0}} and data['owner'] == 0 and data['result'] == "Success" and data['value'] == 'test_value2':
    print("New node 2 PASSED")
else:
    print("New node 2 FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)
r = requests.get("http://localhost:8080/keyValue-store/test3")
expected = {"payload":{"test3":{"10.0.0.15:8080":0}},"result":"Success","value":"test_value3"}
data = r.json()
if json.loads(data['payload']) == {"test3":{"10.0.0.15:8080":0}} and data['owner'] == 0 and data['result'] == "Success" and data['value'] == 'test_value3':
    print("New node 3 PASSED")
else:
    print("New node 3 FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)

r = requests.delete("http://" + servers[0] + "/view", params={'ip_port':'10.0.0.15:8080'})
time.sleep(1)

#################################################################
######                 DELETE/PUT view tests               ######
#################################################################
temp_view = VIEW[1] # save view to put/delete

# DELETE from view
r = requests.delete("http://" + servers[0] + "/view", params={'ip_port':temp_view})
expected = {"result":"Success", "msg":"Successfully removed " + temp_view + " from view"}
expected_status_code = 200
data = r.json()
if data['result'] == 'Success' and data['msg'] == 'Successfully removed ' + temp_view + ' from view':
    print "DELETE from view test: PASSED"
elif r.status_code != expected_status_code:
    print "DELETE from view status code: FAILED"
    print "got:", r.status_code, "\texpected:", expected_status_code
else:
    print("DELETE from view: FAILED")
    print("exp: " + str(expected) + "\nGot: " + r.text)

# DELETE nonexistent process from view
nonexistent_port = "10.0.0.420:8080"
r = requests.delete("http://" + servers[0] + "/view", params={'ip_port': nonexistent_port})
expected = {"result":"Error", "msg":nonexistent_port + " is not in current view"}
expected_status_code = 404
data = r.json()
if data['result'] == 'Error' and data['msg'] == nonexistent_port + ' is not in current view':
    print "DELETE nonexistent process from view test: PASSED"
elif r.status_code != expected_status_code:
    print "DELETE nonexistent process from view status code: FAILED"
    print "got:", r.status_code, "\texpected:", expected_status_code
else:
    print "DELETE nonexistent process from view: FAILED"
    print("exp: " + str(expected) + "\nGot: " + r.text)

# PUT into view
r = requests.put("http://" + servers[0] + "/view", params={'ip_port': temp_view})
expected = {"result":"Success", "msg":"Successfully added " + temp_view + " to view"}
expected_status_code = 200
data = r.json()
if data['result'] == "Success" and data['msg'] == "Successfully added " + temp_view + " to view":
    print "PUT into view test: PASSED"
elif r.status_code != expected_status_code:
    print "PUT into view status code: FAILED"
    print "got:", r.status_code, "\texpected:", expected_status_code
else:
    print "PUT into view: FAILED"
    print("exp: " + str(expected) + "\nGot: " + r.text)

# PUT existing process into view
r = requests.put("http://" + servers[0] + "/view", params={'ip_port': temp_view})
expected = {"result":"Error", "msg":temp_view + " is already in view"}
expected_status_code = 404
data = r.json()
if data['result'] == 'Error' and data['msg'] == temp_view + ' is already in view':
    print "PUT existing process into view test: PASSED"
elif r.status_code != expected_status_code:
    print "PUT existing process into view status code: FAILED"
    print "got:", r.status_code, "\texpected:", expected_status_code
else:
    print "PUT existing process into view: FAILED"
    print("exp: " + str(expected) + "\nGot: " + r.text)

# TEST the GET view for all servers
for c, server in enumerate(servers):
    r = requests.get("http://" + server + "/view")
    expected = {"view":"10.0.0.10:8080,10.0.0.11:8080,10.0.0.12:8080,10.0.0.13:8080,10.0.0.14:8080"}
    expected_status_code = 200
    data = r.json()
    if data['view'] == "10.0.0.10:8080,10.0.0.11:8080,10.0.0.12:8080,10.0.0.13:8080,10.0.0.14:8080":
        print "GET view " + str(c) + " test: PASSED"
    else:
        print "GET view " + str(c) + " : FAILED"
        print("exp: " + str(expected) + "\nGot: " + r.text)
    if r.status_code != expected_status_code:
        print "GET view " + str(c) + " status code: FAILED"
        print "got:", r.status_code, "\texpected:", expected_status_code


#################################################################
######                 multiple server tests               ######
#################################################################

######################### PUT and DELETE ########################
for c, server in enumerate(servers):
    # GET from servers that where put to and from ones that where not
    key = value = server[-4:]
    insert_server = VIEW[c]
    r = requests.put("http://%s/keyValue-store/test_%s" % (server, key), params={"val":"test:" + value})
    # sleep to allow propagation
    time.sleep(10)
    for count, server in enumerate(servers):
        r = requests.get("http://%s/keyValue-store/test_%s" % (server, key))
        expected = {"owner":0,"payload":{"test_" + key:{insert_server:0}},"result":"Success","value":"test:" + value}
        expected_status_code = 200
        data = r.json()
        if json.loads(data['payload']) == {"test_" + key:{insert_server:0}} and data['value'] == "test:" + value and data['owner'] == 0 and data['result'] == "Success":
            print "multiple server PUT - test%d: PASSED" % count
        else:
            print "multiple server PUT - test%d: FAILED" % count
            print("exp: " + str(expected) + "\nGot: " + r.text)
        if r.status_code != expected_status_code:
            print "multiple server PUT status code - test%d: FAILED" % count
            print "got:", r.status_code, "\texpected:", expected_status_code

    # Do the same things with DELETE
    r = requests.delete("http://%s/keyValue-store/test_%s" % (server, key), params={"val":"test:" + value})
    # sleep to allow propagation
    time.sleep(10)
    for count, server in enumerate(servers):
        r = requests.get("http://%s/keyValue-store/test_%s" % (server, key))
        expected = {"payload":{},"result":"Error","error":"Key does not exist"}
        expected_status_code = 404
        data = r.json()
        if json.loads(data['payload']) == {} and data['result'] == "Error" and data['error'] == "Key does not exist":
            print "multiple server DELETE - test%d: PASSED" % count
        else:
            print "multiple server DELETE - test%d: FAILED" % count
            print("exp: " + str(expected) + "\nGot: " + r.text)
        if r.status_code != expected_status_code:
            print "multiple server DELETE status code - test%d: FAILED" % count
            print "got:", r.status_code, "\texpected:", expected_status_code
