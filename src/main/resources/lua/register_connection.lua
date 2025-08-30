local connectionKey = ARGV[1] .. 'connections:' .. ARGV[3]
local routeKey = ARGV[1] .. 'routes:' .. ARGV[3]
local clusterSetKey = ARGV[1] .. 'cluster:connections'
local nodeSetKey = ARGV[1] .. 'nodes:' .. ARGV[2] .. ':connections'
local value = ARGV[2] .. ':' .. ARGV[4]

-- Set connection information
redis.call('SETEX', connectionKey, ARGV[5], value)

-- Set route information
redis.call('SETEX', routeKey, ARGV[5], ARGV[2])

-- Add to cluster connection set
redis.call('SADD', clusterSetKey, ARGV[3])

-- Add to node connection set
redis.call('SADD', nodeSetKey, ARGV[3])

return 1
