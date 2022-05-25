clean:
	cargo clean 


REDIS_PROCESS_NUM = $(shell docker ps|grep redis-server|wc -l)
STOPPED_REDIS_DOCKER = $(shell docker ps -a|grep redis-server|wc -l)

test-start-local-redis:
ifeq ("$(REDIS_PROCESS_NUM)", "1")
	echo "Redis server is already started..."
else
	ifeq ($(STOPPED_REDIS_DOCKER), 1)
		docker start redis-server
	else
		docker run -itd --name redis-server -p 6379:6379 redis
	endif
endif

local-server-run: test-start-local-redis
	cd cmd/skdb-server/; \
		cargo run -- --config ../../skdb_config.yaml 
