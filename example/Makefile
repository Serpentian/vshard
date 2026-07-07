all: stop clean start enter

start:
	tarantoolctl start storage_1_a
	tarantoolctl start storage_1_b
	tarantoolctl start storage_2_a
	tarantoolctl start storage_2_b
	tarantoolctl start router_1
	tarantoolctl start router_2
	tarantoolctl start router_3
	tarantoolctl start router_4
	tarantoolctl start router_5
	tarantoolctl start router_6
	#tarantoolctl start router_7
	#tarantoolctl start router_8
	#tarantoolctl start router_9
	@echo "Waiting cluster to start"
	@sleep 1
	echo "vshard.router.bootstrap({timeout = 30})" | tarantoolctl enter router_1

stop:
	tarantoolctl stop storage_1_a
	tarantoolctl stop storage_1_b
	tarantoolctl stop storage_2_a
	tarantoolctl stop storage_2_b
	tarantoolctl stop router_1
	tarantoolctl stop router_2
	tarantoolctl stop router_3
	tarantoolctl stop router_4
	tarantoolctl stop router_5
	tarantoolctl stop router_6
	# tarantoolctl stop router_7
	# tarantoolctl stop router_8
	# tarantoolctl stop router_9

enter:
	tarantoolctl enter router_1

logcat:
	tail -f data/*.log

clean:
	rm -rf data/

load:
	tarantoolctl start generate_load.lua

.PHONY: console test deploy clean
