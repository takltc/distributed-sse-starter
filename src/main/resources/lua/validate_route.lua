local prefix = ARGV[1]
local connectionId = ARGV[2]
local expectedNode = ARGV[3]

-- Get current route information
local routeKey = prefix .. 'routes:' .. connectionId
local currentNode = redis.call('GET', routeKey)

-- If route information is inconsistent, update to correct node
if currentNode and currentNode ~= expectedNode then
    redis.call('SETEX', routeKey, ARGV[4], expectedNode)
    return 1
end

return 0
