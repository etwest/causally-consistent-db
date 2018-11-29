curl -X PUT http://localhost:8080/keyValue-store/69 -d val="123"
curl -X GET http://localhost:8080/keyValue-store/69 -d payload="{\"69\":{\"10.0.0.10:8080\":34}}"
curl -X DELETE http://localhost:8080/keyValue-store/69 -d payload="{\"69\":{\"10.0.0.10:8080\":0}}"