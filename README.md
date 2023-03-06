# CSE 138: Assignment 3 - An Available Fault-Tolerant Key Value Store by Yesh Chandiramani, Myles Mansfield, Eli Warres

## Assignment Notes:
- Used a grace day (submitted on Fri Mar 3 at 11:15 PM PST)

## Implementation Strategy
- using vector clocks for causal consistency
- using a custom replication protocol (similar to quorom)


## File Structure
- src contains all source code
  - config: getting env variables
  - controllers: logic for how to handle requests
    - kvs.controller: KVS route request handler logic
  - dtos: data transfer objects (dtos) are used for validation of data being sent in a request body field
  - execptions: logic for handling an HTTP exeception
  - http: files for vscode's [rest-client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) API testing
  - interfaces: definitions of interfaces/structs (data objects being used in code)
  - io: socket.io server
  - middlewares: logic for middlewares being used
  - models: definitions of data models (data stored in memory)
  - routes: definitions of all routes that the server will respond to
  - services: logic for handling operations
    - kvs.service: logic for manipulating KVS operations
  - tests: unit testing logic
    - kvs.test: all the unit tests for kvs routes
  - utils: utility helper functions used throughout the project
  - app.ts: express app definitions
  - server.ts: logic for bootstrapping the App module
    - used for inserting new routes

## Install and run
1. npm install
2. npm run dev

## Unit Tests
1. npm run test

## Running using Docker
1. `make network` (only have to do this once to establish the kv_subnet network)
2. `make build` (builds the kvs:2.0 docker image)
3. `make reset` (removes exisiting containers named kvs-replica-*)
4. `make run_replica_1` (run replica 1 on port 8080)
5. `make run_replica_2` (run replica 2 on port 8081)
6. `make run_replica_3` (run replica 3 on port 8082)

## Docker Errors
- network already exists
    - fix: do nothing
- container with the same name already exists
    - fix:
        - remove that container `docker stop <container_name> && docker rm <container_name>`

## Side Notes
- exposing .env files because they are needed to run the project and they are not secrets
