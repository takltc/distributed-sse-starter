local prefix = ARGV[1]
local nodeName = ARGV[2]
local maxAge = tonumber(ARGV[3])
local currentTime = tonumber(ARGV[4])

-- Get node connection set
local nodeSetKey = prefix .. 'nodes:' .. nodeName .. ':connections'
local connections = redis.call('SMEMBERS', nodeSetKey)

local removedCount = 0
for i, connectionId in ipairs(connections) do
    local connectionKey = prefix .. 'connections:' .. connectionId
    local value = redis.call('GET', connectionKey)

    if value then
        local parts = {}
        for part in string.gmatch(value, '[^:]+') do
            table.insert(parts, part)
        end

        if #parts >= 2 then
            local timestamp = tonumber(parts[2])
            if currentTime - timestamp > maxAge then
                -- Connection expired, perform cleanup
                local routeKey = prefix .. 'routes:' .. connectionId
                local clusterSetKey = prefix .. 'cluster:connections'

                redis.call('DEL', connectionKey)
                redis.call('DEL', routeKey)
                redis.call('SREM', clusterSetKey, connectionId)
                redis.call('SREM', nodeSetKey, connectionId)

                removedCount = removedCount + 1
            end
        end
    else
        -- Connection information does not exist, remove from set
        redis.call('SREM', nodeSetKey, connectionId)
    end
end

return removedCount
