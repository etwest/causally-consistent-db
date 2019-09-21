## What is?
This codebase creates a number of docker containers which each act as nodes within a distributed key value store. This key value store replicates the data given to it with the goal of maintaining data intergity even if a node is lost. Additionally, nodes are seperated into different shards where nodes within the same shards are replicated and nodes in different shards do not share data. This allows the database to scale out and operate faster, while maintaining replication. When nodes are deleted or added (via http commands) the database automatically moves nodes between shards to keep them balanced. The database may even automatically delete a shard and reportion its data if their are too few nodes to support that many shards.

## How to Run?
Run the bash file `build.sh` to create the docker image and then create 5 containers running the app. Then a variety of http requests can be made to add key-value pairs, search, get, delete, etc.

## Files
The most important file is `app.py` this file is the flask application which runs the http server. The other relevant files are `broadcast.py` and `Dockerfile` all the other files are notes or for testing.

Edit the `build.sh` file to change how many shards are created and what their assigned shard ID is. Assigned ID is the parameter `S`.
## Contributors
- Biawan Huang
- Bryan Ji
- Eugene Chou
- Evan West
