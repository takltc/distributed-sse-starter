local connectionKey = ARGV[1] .. 'connections:' .. ARGV[3]
local routeKey = ARGV[1] .. 'routes:' .. ARGV[3]
local clusterSetKey = ARGV[1] .. 'cluster:connections'
local nodeSetKey = ARGV[1] .. 'nodes:' .. ARGV[2] .. ':connections'

-- Delete connection information
redis.call('DEL', connectionKey)

-- Delete route information
redis.call('DEL', routeKey)

-- Remove from cluster connection set
redis.call('SREM', clusterSetKey, ARGV[3])

-- Remove from node connection set
redis.call('SREM', nodeSetKey, ARGV[3])

return 1
