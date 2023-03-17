# app name should be overridden.
# ex) production-stage: make build APP_NAME=<APP_NAME>
# ex) development-stage: make build-dev APP_NAME=<APP_NAME>

APP_NAME = kvs:4.0

.PHONY: build
# Make Docker Network
network:
	docker network create --subnet=10.10.0.0/16 kv_subnet

# Build the container image - Production
build:
	docker build -t ${APP_NAME}\
		--target production-build-stage .

# Clean the container image
clean:
	docker rmi -f ${APP_NAME}

# Run the replicas
run_replica_1:
	docker run --publish 8080:8080 \
	--net=kv_subnet \
	--ip=10.10.0.2 \
	--name="kvs-replica-1" \
	--env ADDRESS="10.10.0.2:8080" \
	${APP_NAME}
run_replica_2:
	docker run --publish 8081:8080 \
	--net=kv_subnet \
	--ip=10.10.0.3 \
	--name="kvs-replica-2" \
	--env ADDRESS="10.10.0.3:8080" \
	${APP_NAME}

run_replica_3:
	docker run --publish 8082:8080 \
	--net=kv_subnet \
	--ip=10.10.0.4 \
	--name="kvs-replica-3" \
	--env ADDRESS="10.10.0.4:8080" \
	${APP_NAME}

reset:
	docker rm -f kvs-replica-1 kvs-replica-2 kvs-replica-3

all: build
