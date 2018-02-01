test_run = require('test_run').new()

REPLICASET_1 = { 'storage_1_a', 'storage_1_b' }
REPLICASET_2 = { 'storage_2_a', 'storage_2_b' }

test_run:create_cluster(REPLICASET_1, 'main')
test_run:create_cluster(REPLICASET_2, 'main')
util = require('util')
util.wait_master(test_run, REPLICASET_1, 'storage_1_a')
util.wait_master(test_run, REPLICASET_2, 'storage_2_a')
test_run:cmd("create server router_1 with script='main/router_1.lua'")
test_run:cmd("start server router_1")
test_run:cmd('switch router_1')
util = require('util')

rs1 = vshard.router.internal.replicasets[replicasets[1]]
rs2 = vshard.router.internal.replicasets[replicasets[2]]

util.collect_timeouts(rs1)
util.collect_timeouts(rs2)

--
-- Test a case, when timeout is already minimal and tries to
-- decrease.
--
for i = 1, 9 do rs1:callrw('echo') end
util.collect_timeouts(rs1)
_ = rs1:callrw('echo')
util.collect_timeouts(rs1)

--
-- Test oscillation protection. When a timeout is increased, it
-- must not decrease until 10 success requests.
--
for i = 1, 2 do rs1:callrw('sleep', {vshard.consts.CALL_TIMEOUT_MIN + 0.1}) end
util.collect_timeouts(rs1)

for i = 1, 9 do rs1:callrw('echo') end
util.collect_timeouts(rs1)
_ = rs1:callrw('sleep', {vshard.consts.CALL_TIMEOUT_MIN * 2 + 0.1})
util.collect_timeouts(rs1)
-- Ok, because new timeout is increased twice.
_ = rs1:callrw('sleep', {vshard.consts.CALL_TIMEOUT_MIN * 1.8})
util.collect_timeouts(rs1)
for i = 1, 9 do rs1:callrw('echo') end
util.collect_timeouts(rs1)

_ = test_run:cmd("switch default")
test_run:cmd("stop server router_1")
test_run:cmd("cleanup server router_1")
test_run:drop_cluster(REPLICASET_1)
test_run:drop_cluster(REPLICASET_2)