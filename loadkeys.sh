#!/bin/bash -e


for key in $(cat ./keys.txt); do curl -X PUT http://localhost:8082/keyValue-store/${key} -d "val=hello"; done