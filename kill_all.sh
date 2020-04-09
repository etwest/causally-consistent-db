# Code Written by Evan West
# With contributions from: Biawan Huang, Bryan Ji, and Eugene Chou
# Publicly posted to github
# https://github.com/etwest

docker kill $(docker ps -aq)
docker rm $(docker ps -aq)
docker network rm mynet