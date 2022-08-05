clean:
	cargo clean 


REDIS_PROCESS_NUM = $(shell docker ps|grep redis-server|wc -l)
STOPPED_REDIS_DOCKER = $(shell docker ps -a|grep redis-server|wc -l)

test-start-local-redis:
ifeq ("$(REDIS_PROCESS_NUM)", "1")
	@echo "Redis server is already started..."
else
	ifeq ($(STOPPED_REDIS_DOCKER), 1)
		docker start redis-server
	else
		docker run -itd --name redis-server -p 6379:6379 redis
	endif
endif

local-server-run: test-start-local-redis
	@cd cmd/skdb-server/; \
		cargo run -- --config ../../skdb_config.yaml 

mock-multi: test-start-local-redis
	if [ $$num ]; \
		then echo $$num"'s instance is waiting to start..." && \
        num2=1 ; while [[ $$num2 -le $$num ]] ; do \
            eval "nohup cargo run --bin server -- env -e dev -g 999"$$num2\
				" -i /tmp/skdb"$$num2" -l /tmp/skdb"$$num2" -n skdb_server"$$num2" -r 127.0.0.1:6379 > /dev/null 2>&1 &" \
				&& echo $$num2 \
				&& ((num2 = num2 + 1)); \
        done ; \
	else \
		echo "Num is not config, use default 3 instance " && \
		num=3 && echo "start" \
        num2=1 ; while [[ $$num2 -le num ]] ; do \
            eval "nohup cargo run --bin server -- env -e dev -g 999"$$num2\
				" -i /tmp/skdb"$$num2" -l /tmp/skdb"$$num2" -n skdb_server"$$num2" -r 127.0.0.1:6379 > /dev/null 2>&1 &" \
				&& ((num2 = num2 + 1)) \
				&& echo "Starting server"$$num2" ..."; \
        done ; \
	fi

mock-multi-write-test: mock-multi
	@sleep 5
	@echo "Start integration test case test_write_only_client..."
	cargo test test_write_only_client --features fail/failpoints -- --nocapture --ignored --test-threads 1
	@echo "Integration test case test_write_only_client execute finished..."
	@echo "Start to kill skdb server..."
	pkill -f skdb_server

kill-server: 
	pkill -f skdb_server 

test: 
	CLOSE_LOG=1 cargo test $(name) --features fail/failpoints -- --nocapture

testi: 
	cargo test $(name) --features fail/failpoints -- --nocapture --ignored --test-threads 1