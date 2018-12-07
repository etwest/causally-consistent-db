./kill_all.sh
docker build -t hw4 .
# export VIEW="10.0.0.10:8080"
export VIEW="10.0.0.10:8080,10.0.0.11:8080,10.0.0.12:8080,10.0.0.13:8080,10.0.0.14:8080,10.0.0.15:8080"
# export VIEW='10.0.0.10:8080'

sudo docker network create --subnet=10.0.0.10/16 mynet
docker run -d -p 8080:8080 --net=mynet -e IP_PORT="10.0.0.10:8080" -e VIEW=$VIEW -e S="3" --ip=10.0.0.10 hw4
docker run -d -p 8081:8080 --net=mynet -e IP_PORT="10.0.0.11:8080" -e VIEW=$VIEW -e S="3" --ip=10.0.0.11 hw4
docker run -d -p 8082:8080 --net=mynet -e IP_PORT="10.0.0.12:8080" -e VIEW=$VIEW -e S="3" --ip=10.0.0.12 hw4
docker run -d -p 8083:8080 --net=mynet -e IP_PORT="10.0.0.13:8080" -e VIEW=$VIEW -e S="3" --ip=10.0.0.13 hw4
docker run -d -p 8084:8080 --net=mynet -e IP_PORT="10.0.0.14:8080" -e VIEW=$VIEW -e S="3" --ip=10.0.0.14 hw4
docker run -d -p 8085:8080 --net=mynet -e IP_PORT="10.0.0.15:8080" -e VIEW=$VIEW -e S="3" --ip=10.0.0.15 hw4