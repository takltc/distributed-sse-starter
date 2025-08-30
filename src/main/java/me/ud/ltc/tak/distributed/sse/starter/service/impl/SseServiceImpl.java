package me.ud.ltc.tak.distributed.sse.starter.service.impl;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import me.ud.ltc.tak.distributed.sse.starter.config.SseProperties;
import me.ud.ltc.tak.distributed.sse.starter.script.LuaScript;
import me.ud.ltc.tak.distributed.sse.starter.script.LuaScriptService;
import me.ud.ltc.tak.distributed.sse.starter.service.SseService;

import lombok.extern.slf4j.Slf4j;

/**
 * Server-Sent Events service implementation
 * 
 * @author takltc
 */
@Slf4j
@Service
public class SseServiceImpl implements SseService {

    private final SseProperties sseProperties;
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisMessageListenerContainer redisMessageListenerContainer;
    private final LuaScriptService luaScriptService;
    private final String nodeName;

    public SseServiceImpl(SseProperties sseProperties, RedisTemplate<String, Object> redisTemplate,
        RedisMessageListenerContainer redisMessageListenerContainer, LuaScriptService luaScriptService,
        String nodeName) {
        this.sseProperties = sseProperties;
        this.redisTemplate = redisTemplate;
        this.redisMessageListenerContainer = redisMessageListenerContainer;
        this.luaScriptService = luaScriptService;
        this.nodeName = nodeName;
    }

    private final Map<String, SseEmitter> connections = new ConcurrentHashMap<>();
    private final Map<String, Long> connectionTimes = new ConcurrentHashMap<>();
    private volatile ScheduledExecutorService cleanupExecutor;
    private volatile ScheduledExecutorService heartbeatExecutor;

    // Resource recycling monitoring metrics
    private final AtomicInteger successfulCleanupCount = new AtomicInteger(0);
    private final AtomicInteger failedCleanupCount = new AtomicInteger(0);
    private final AtomicInteger retryAttemptCount = new AtomicInteger(0);

    // Shutdown flag
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /**
     * Unified retry limit to prevent extreme configurations from causing long blocking or integer overflow
     */
    private static final int MAX_ALLOWED_RETRIES = 1000;
    /**
     * Retry log throttling: only print WARN in the first few attempts, fixed steps, and the last attempt
     */
    private static final int RETRY_LOG_VERBOSE_HEAD = 3;
    private static final int RETRY_LOG_STEP = 50;

    /**
     * Validate configuration parameters
     */
    private void validateConfiguration() {
        // Validate connection configuration
        if (sseProperties.getConnection().getTimeout() <= 0) {
            throw new IllegalStateException("Connection timeout must be greater than 0");
        }
        if (sseProperties.getConnection().getHeartbeat() <= 0) {
            throw new IllegalStateException("Heartbeat interval must be greater than 0");
        }
        if (sseProperties.getConnection().getMaxConnections() <= 0) {
            throw new IllegalStateException("Maximum connections must be greater than 0");
        }

        // Validate cleanup configuration
        if (sseProperties.getCleanup().isEnabled()) {
            if (sseProperties.getCleanup().getInterval() <= 0) {
                throw new IllegalStateException("Cleanup interval must be greater than 0");
            }
            if (sseProperties.getCleanup().getConnectionTtl() <= 0) {
                throw new IllegalStateException("Connection TTL must be greater than 0");
            }
        }

        // Validate Redis configuration
        if (sseProperties.getRedis().isEnabled()) {
            if (sseProperties.getRedis().getConnectionTtl() <= 0) {
                throw new IllegalStateException("Redis connection TTL must be greater than 0");
            }
            if (sseProperties.getRedis().getPrefix() == null || sseProperties.getRedis().getPrefix().isEmpty()) {
                throw new IllegalStateException("Redis key prefix cannot be empty");
            }
        }

        // Validate consistency configuration
        if (sseProperties.getConsistency().isEnabled()) {
            if (sseProperties.getConsistency().getStatusCheckInterval() <= 0) {
                throw new IllegalStateException("Status check interval must be greater than 0");
            }
            if (sseProperties.getConsistency().isInterNodeSyncEnabled()
                && sseProperties.getConsistency().getRouteValidationInterval() <= 0) {
                throw new IllegalStateException("Route validation interval must be greater than 0");
            }
        }

        // Validate reliability configuration
        if (sseProperties.getReliability().isResourceLeakDetection()
            && sseProperties.getReliability().getLeakDetectionInterval() <= 0) {
            throw new IllegalStateException("Resource leak detection interval must be greater than 0");
        }
    }

    /**
     * Create named scheduled thread pool
     * 
     * @param corePoolSize Core thread count
     * @param name Thread pool name
     * @return Scheduled thread pool executor
     */
    private ScheduledExecutorService createNamedScheduledThreadPool(int corePoolSize, String name) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(corePoolSize, r -> {
            Thread t = new Thread(r, name + "-thread-" + System.currentTimeMillis());
            t.setDaemon(false);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    @PostConstruct
    public void init() {
        // Validate configuration
        validateConfiguration();

        // Initialize thread pools
        cleanupExecutor = createNamedScheduledThreadPool(2, "sse-cleanup");
        int heartbeatThreads = Math.max(2, Math.min(10, sseProperties.getConnection().getMaxConnections() / 50));
        heartbeatExecutor = createNamedScheduledThreadPool(heartbeatThreads, "sse-heartbeat");

        // Start cleanup task
        if (sseProperties.getCleanup().isEnabled()) {
            cleanupExecutor.scheduleAtFixedRate(this::cleanupExpiredConnections,
                sseProperties.getCleanup().getInterval(), sseProperties.getCleanup().getInterval(),
                TimeUnit.MILLISECONDS);
            log.info("Expired connection cleanup task started, interval: {}ms", sseProperties.getCleanup().getInterval());
        }

        // Start consistency check task
        if (sseProperties.getConsistency().isEnabled() && sseProperties.getRedis().isEnabled()) {
            cleanupExecutor.scheduleAtFixedRate(this::checkConnectionConsistency,
                sseProperties.getConsistency().getStatusCheckInterval(),
                sseProperties.getConsistency().getStatusCheckInterval(), TimeUnit.MILLISECONDS);
            log.info("Connection consistency check task started, interval: {}ms", sseProperties.getConsistency().getStatusCheckInterval());
        }

        // Start resource leak detection task
        if (sseProperties.getReliability().isResourceLeakDetection()) {
            cleanupExecutor.scheduleAtFixedRate(this::detectResourceLeaks,
                sseProperties.getReliability().getLeakDetectionInterval(),
                sseProperties.getReliability().getLeakDetectionInterval(), TimeUnit.MILLISECONDS);
            log.info("Resource leak detection task started, interval: {}ms", sseProperties.getReliability().getLeakDetectionInterval());
        }

        // Start active route validation task
        if (sseProperties.getConsistency().isEnabled() && sseProperties.getConsistency().isInterNodeSyncEnabled()
            && sseProperties.getRedis().isEnabled()) {
            cleanupExecutor.scheduleAtFixedRate(this::validateConnectionRoutes,
                sseProperties.getConsistency().getRouteValidationInterval(),
                sseProperties.getConsistency().getRouteValidationInterval(), TimeUnit.MILLISECONDS);
            log.info("Connection route validation task started, interval: {}ms", sseProperties.getConsistency().getRouteValidationInterval());
        }

        if (sseProperties.getRedis().isEnabled()) {
            setupRedisListener();
            // Clean up connection information for this node at startup
            cleanupNodeConnections();
        }

        log.info("SSE service initialization completed, thread pool configuration - cleanup threads: {}, heartbeat threads: {}", 2, heartbeatThreads);
    }

    /**
     * Check connection consistency
     */
    private void checkConnectionConsistency() {
        try {
            if (!sseProperties.getConsistency().isEnabled()) {
                return;
            }

            log.debug("Start checking connection status consistency");

            // Check consistency between local connections and records in Redis
            String nodeSetKey = sseProperties.getRedis().getPrefix() + "nodes:" + getNodeName() + ":connections";
            Set<Object> rawConnections = redisTemplate.opsForSet().members(nodeSetKey);
            Set<String> redisConnections = null;
            if (rawConnections != null) {
                redisConnections = rawConnections.stream().map(Object::toString).collect(Collectors.toSet());
            }

            if (redisConnections != null) {
                // Check if there are connections in Redis but not locally (orphan connections)
                for (String connectionId : redisConnections) {
                    if (!connections.containsKey(connectionId)) {
                        log.warn("Found orphan connection: {}", connectionId);
                        // Decide handling strategy based on configuration
                        if ("auto_correct".equals(sseProperties.getConsistency().getInconsistencyHandlingStrategy())) {
                            // Auto-correct: Remove orphan connection from Redis
                            unregisterConnectionFromRedisWithRetry(connectionId);
                        }
                    }
                }

                // Check if there are connections locally but not in Redis (Redis record loss)
                for (String connectionId : connections.keySet()) {
                    if (!redisConnections.contains(connectionId)) {
                        log.warn("Found local connection missing in Redis: {}", connectionId);
                        if ("auto_correct".equals(sseProperties.getConsistency().getInconsistencyHandlingStrategy())) {
                            // Auto-correct: Re-register to Redis
                            registerConnectionInRedis(connectionId);
                        }
                    }
                }
            }

            log.debug("Connection status consistency check completed");
        } catch (Exception e) {
            log.error("Connection status consistency check error", e);
        }
    }

    /**
     * Validate connection route information
     */
    private void validateConnectionRoutes() {
        try {
            if (!sseProperties.getConsistency().isEnabled() || !sseProperties.getConsistency().isInterNodeSyncEnabled()
                || !sseProperties.getRedis().isEnabled()) {
                return;
            }

            log.debug("Start validating connection route information");

            String nodeName = getNodeName();
            String prefix = sseProperties.getRedis().getPrefix();
            int ttl = sseProperties.getRedis().getConnectionTtl();

            // Traverse local connections and validate route information
            for (String connectionId : connections.keySet()) {
                try {
                    // Use Lua script to validate route information
                    Long result = luaScriptService.executeScript(LuaScript.VALIDATE_ROUTE.name(), new ArrayList<>(),
                        Arrays.asList(prefix, connectionId, nodeName, String.valueOf(ttl)), ReturnType.INTEGER);

                    if (result != null && result > 0) {
                        log.info("Fixed route information for connection {}", connectionId);
                    }
                } catch (Exception e) {
                    log.warn("Error validating route information for connection {}", connectionId, e);
                }
            }

            log.debug("Connection route information validation completed");
        } catch (Exception e) {
            log.error("Connection route information validation error", e);
        }
    }

    /**
     * Detect resource leaks
     */
    private void detectResourceLeaks() {
        try {
            if (!sseProperties.getReliability().isResourceLeakDetection()) {
                return;
            }

            log.debug("Start detecting resource leaks");

            // Check consistency between connection time and cleanup time
            long now = System.currentTimeMillis();
            long maxAge = sseProperties.getCleanup().getConnectionTtl();

            // Check if there are connections that have timed out but have not been cleaned up
            connectionTimes.entrySet().removeIf(entry -> {
                if (now - entry.getValue() > maxAge) {
                    log.warn("Found connection that timed out but was not cleaned up: {}", entry.getKey());
                    closeConnection(entry.getKey());
                    return true;
                }
                return false;
            });

            log.debug("Resource leak detection completed");
        } catch (Exception e) {
            log.error("Resource leak detection error", e);
        }
    }

    private void setupRedisListener() {
        // Listen for broadcast messages
        ChannelTopic topic = new ChannelTopic(sseProperties.getRedis().getChannel());
        redisMessageListenerContainer.addMessageListener((message, pattern) -> {
            try {
                String messageBody = new String(message.getBody());
                broadcast("redis", messageBody);
            } catch (Exception e) {
                log.error("Error processing Redis message", e);
            }
        }, topic);

        // Listen for control commands
        ChannelTopic commandTopic =
            new ChannelTopic(sseProperties.getRedis().getPrefix() + "commands:" + getNodeName());
        redisMessageListenerContainer.addMessageListener((message, pattern) -> {
            try {
                String messageBody = new String(message.getBody());
                handleCommand(messageBody);
            } catch (Exception e) {
                log.error("Error processing Redis control command", e);
            }
        }, commandTopic);
    }

    @Override
    public SseEmitter createConnection(String connectionId) {
        log.info("Creating SSE connection: {}", connectionId);

        // Handle null connection ID case
        if (connectionId == null) {
            log.warn("Connection ID is null, cannot create connection");
            return null;
        }

        // Check maximum connection limit (allow connections with same ID to be replaced, not counted toward limit)
        if (!connections.containsKey(connectionId)
            && connections.size() >= sseProperties.getConnection().getMaxConnections()) {
            log.warn("Maximum connection limit reached: {}", sseProperties.getConnection().getMaxConnections());
            return null;
        }

        SseEmitter emitter = new SseEmitter((long)sseProperties.getConnection().getTimeout());

        connections.put(connectionId, emitter);
        connectionTimes.put(connectionId, System.currentTimeMillis());

        setupEmitterCallbacks(connectionId, emitter);
        startHeartbeat(connectionId, emitter);

        if (sseProperties.getRedis().isEnabled()) {
            registerConnectionInRedis(connectionId);
        }

        return emitter;
    }

    private void setupEmitterCallbacks(String connectionId, SseEmitter emitter) {
        emitter.onCompletion(() -> {
            log.info("SSE connection completed: {}", connectionId);
            removeConnection(connectionId);
        });

        emitter.onTimeout(() -> {
            log.info("SSE connection timed out: {}", connectionId);
            removeConnection(connectionId);
        });

        emitter.onError(throwable -> {
            log.error("SSE connection error: {}, connection info: [current connections: {}, max connections: {}]", connectionId, connections.size(),
                sseProperties.getConnection().getMaxConnections(), throwable);
            removeConnection(connectionId);
        });
    }

    private void startHeartbeat(String connectionId, SseEmitter emitter) {
        // Ensure heartbeat executor is initialized (may not be called in test environment)
        if (heartbeatExecutor == null) {
            synchronized (this) {
                if (heartbeatExecutor == null) {
                    int heartbeatThreads =
                        Math.max(2, Math.min(10, sseProperties.getConnection().getMaxConnections() / 50));
                    heartbeatExecutor = createNamedScheduledThreadPool(heartbeatThreads, "sse-heartbeat");
                    log.info("Initializing heartbeat executor, thread count: {}", heartbeatThreads);
                }
            }
        }

        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                if (connections.containsKey(connectionId)) {
                    SseEmitter.SseEventBuilder event = SseEmitter.event().name("heartbeat").data("ping")
                        .id(String.valueOf(System.currentTimeMillis()));
                    emitter.send(event);
                }
            } catch (Exception e) {
                log.error("Unable to send heartbeat to connection: {}, connection status: [exists: {}, current connections: {}]", connectionId,
                    connections.containsKey(connectionId), connections.size(), e);
                removeConnection(connectionId);
            }
        }, sseProperties.getConnection().getHeartbeat(), sseProperties.getConnection().getHeartbeat(),
            TimeUnit.MILLISECONDS);
    }

    private void registerConnectionInRedis(String connectionId) {
        // Check if Redis atomic operations and connection registration atomicity guarantees are enabled
        if (!sseProperties.getAtomicOperation().isRedisAtomicEnabled()
            || !sseProperties.getAtomicOperation().isConnectionRegistrationAtomic()) {
            fallbackRegisterConnection(connectionId);
            return;
        }

        String nodeName = getNodeName();
        long timestamp = System.currentTimeMillis();
        int ttl = sseProperties.getRedis().getConnectionTtl();
        int maxRetries = sseProperties.getAtomicOperation().getLuaScriptRetryCount();
        int retryInterval = sseProperties.getAtomicOperation().getLuaScriptRetryInterval();

        // Use unified retry mechanism to execute Lua script
        boolean success = executeWithRetry(() -> {
            luaScriptService.executeScript(LuaScript.REGISTER_CONNECTION.name(), new ArrayList<>(),
                Arrays.asList(sseProperties.getRedis().getPrefix(), nodeName, connectionId, String.valueOf(timestamp),
                    String.valueOf(ttl)),
                ReturnType.INTEGER);
            log.debug("Atomic connection registration successful: {}", connectionId);
        }, maxRetries, retryInterval, "atomic connection registration");

        if (!success) {
            // All retries failed, fall back to original non-atomic operation
            fallbackRegisterConnection(connectionId);
        }
    }

    private void fallbackRegisterConnection(String connectionId) {
        int maxRetries = sseProperties.getAtomicOperation().getLuaScriptRetryCount();
        int retryInterval = sseProperties.getAtomicOperation().getLuaScriptRetryInterval();

        // Use unified retry mechanism to execute registration operation
        boolean success = executeWithRetry(() -> {
            String key = sseProperties.getRedis().getPrefix() + "connections:" + connectionId;
            String value = getNodeName() + ":" + System.currentTimeMillis();

            redisTemplate.opsForValue().set(key, value, sseProperties.getRedis().getConnectionTtl(), TimeUnit.SECONDS);

            // Register route information simultaneously
            registerRoute(connectionId);

            // Add to cluster connection set
            String clusterSetKey = sseProperties.getRedis().getPrefix() + "cluster:connections";
            String nodeSetKey = sseProperties.getRedis().getPrefix() + "nodes:" + getNodeName() + ":connections";
            redisTemplate.opsForSet().add(clusterSetKey, connectionId);
            redisTemplate.opsForSet().add(nodeSetKey, connectionId);

            log.debug("Fallback connection registration successful: {}", connectionId);
        }, maxRetries, retryInterval, "fallback connection registration");

        if (!success) {
            log.error("Fallback connection registration finally failed: {}", connectionId);
        }
    }

    private void unregisterConnectionFromRedisWithRetry(String connectionId) {
        // Check if Redis atomic operations and connection unregistration atomicity guarantees are enabled
        if (!sseProperties.getAtomicOperation().isRedisAtomicEnabled()
            || !sseProperties.getAtomicOperation().isConnectionUnregistrationAtomic()) {
            fallbackUnregisterConnectionWithRetry(connectionId);
            return;
        }

        String nodeName = getNodeName();
        int maxRetries = sseProperties.getReliability().getCleanupRetryCount();
        int retryInterval = sseProperties.getReliability().getCleanupRetryInterval();

        // Record retry attempt count
        retryAttemptCount.incrementAndGet();

        // Use unified retry mechanism to execute Lua script
        boolean success = executeWithRetry(() -> {
            Long result = luaScriptService.executeScript(LuaScript.UNREGISTER_CONNECTION.name(), new ArrayList<>(),
                Arrays.asList(sseProperties.getRedis().getPrefix(), nodeName, connectionId), ReturnType.INTEGER);

            if (result != null && result > 0) {
                log.info("Atomic connection unregistration successful: {}", connectionId);
                successfulCleanupCount.incrementAndGet();
            } else {
                log.warn("Atomic connection unregistration returned abnormal result: {}, result: {}", connectionId, result);
                throw new RuntimeException("Atomic connection unregistration returned abnormal result");
            }
        }, maxRetries, retryInterval, "atomic connection unregistration");

        if (!success) {
            // All retries failed, fall back to original non-atomic operation
            fallbackUnregisterConnectionWithRetry(connectionId);
        }
    }

    private void fallbackUnregisterConnectionWithRetry(String connectionId) {
        int maxRetries = sseProperties.getReliability().getCleanupRetryCount();
        int retryInterval = sseProperties.getReliability().getCleanupRetryInterval();

        // Record retry attempt count
        retryAttemptCount.incrementAndGet();

        // Use unified retry mechanism to execute unregistration operation
        boolean success = executeWithRetry(() -> {
            String key = sseProperties.getRedis().getPrefix() + "connections:" + connectionId;
            redisTemplate.delete(key);

            // Unregister route information simultaneously
            unregisterRoute(connectionId);

            // Remove from cluster connection set
            String clusterSetKey = sseProperties.getRedis().getPrefix() + "cluster:connections";
            String nodeSetKey = sseProperties.getRedis().getPrefix() + "nodes:" + getNodeName() + ":connections";
            redisTemplate.opsForSet().remove(clusterSetKey, connectionId);
            redisTemplate.opsForSet().remove(nodeSetKey, connectionId);

            log.info("Fallback connection unregistration successful: {}", connectionId);
            successfulCleanupCount.incrementAndGet();
        }, maxRetries, retryInterval, "fallback connection unregistration");

        if (!success) {
            log.error("Fallback connection unregistration finally failed: {}", connectionId);
        }
    }

    @Override
    public void sendMessage(String connectionId, String eventName, Object data) {
        SseEmitter emitter = connections.get(connectionId);
        if (emitter != null) {
            try {
                SseEmitter.SseEventBuilder event =
                    SseEmitter.event().name(eventName).data(data).id(String.valueOf(System.currentTimeMillis()));
                emitter.send(event);
            } catch (Exception e) {
                log.error("Unable to send message to connection: {}, connection status: [exists: {}, current connections: {}]", connectionId,
                    connections.containsKey(connectionId), connections.size(), e);
                removeConnection(connectionId);
            }
        }
    }

    @Override
    public void broadcast(String eventName, Object data) {
        connections.forEach((connectionId, emitter) -> {
            try {
                SseEmitter.SseEventBuilder event =
                    SseEmitter.event().name(eventName).data(data).id(String.valueOf(System.currentTimeMillis()));
                emitter.send(event);
            } catch (Exception e) {
                log.error("Unable to broadcast message to connection: {}, connection status: [exists: {}, current connections: {}]", connectionId,
                    connections.containsKey(connectionId), connections.size(), e);
                removeConnection(connectionId);
            }
        });
    }

    /**
     * Send message to specified connection (supports cross-node)
     * 
     * @param connectionId Connection ID
     * @param eventName Event name
     * @param data Data
     */
    @Override
    public void sendMessageToConnection(String connectionId, String eventName, Object data) {
        // Check if it is a local connection
        if (connections.containsKey(connectionId)) {
            sendMessage(connectionId, eventName, data);
            return;
        }

        // If not a local connection, send to the corresponding node through route information
        if (sseProperties.getRedis().isEnabled()) {
            String targetNode = getRoute(connectionId);
            if (targetNode != null && !targetNode.equals(getNodeName())) {
                // Send command to target node
                String command = String.format("SEND_MESSAGE:%s:%s:%s", connectionId, eventName, data);
                sendCommand(targetNode, command);
                return;
            }
        }

        log.warn("Cannot find connection {}: neither a local connection nor has corresponding route information", connectionId);
    }

    @Override
    public void broadcastToCluster(String eventName, Object data) {
        if (sseProperties.getRedis().isEnabled()) {
            redisTemplate.convertAndSend(sseProperties.getRedis().getChannel(), eventName + ":" + data);
        }
        broadcast(eventName, data);
    }

    @Override
    public void closeConnection(String connectionId) {
        // Handle null connection ID case
        if (connectionId == null) {
            log.warn("Attempted to close null connection ID");
            return;
        }

        // Check if it is a local connection
        if (connections.containsKey(connectionId)) {
            SseEmitter emitter = connections.get(connectionId);
            if (emitter != null) {
                try {
                    emitter.complete();
                } catch (Exception e) {
                    log.error("Error closing connection: {}", connectionId, e);
                }
                removeConnection(connectionId);
            }
            return;
        }

        // If not a local connection, send close connection command to the corresponding node through route information
        if (sseProperties.getRedis().isEnabled()) {
            String targetNode = getRoute(connectionId);
            if (targetNode != null && !targetNode.equals(getNodeName())) {
                // Send close connection command to target node
                String command = "CLOSE_CONNECTION:" + connectionId;
                sendCommand(targetNode, command);
                return;
            }
        }

        log.warn("Cannot find connection {}: neither a local connection nor has corresponding route information", connectionId);
    }

    @Override
    public void closeAllConnections() {
        log.info("Start closing all connections, current connection count: {}", connections.size());

        final AtomicInteger closedCount = new AtomicInteger(0);
        connections.forEach((connectionId, emitter) -> {
            try {
                if (!shuttingDown.get()) {
                    // Send close notification when not in shutdown state
                    emitter.send(SseEmitter.event().name("system").data("connection_closed"));
                }
                emitter.complete();
                closedCount.incrementAndGet();
                log.debug("Connection closed: {}", connectionId);
            } catch (Exception e) {
                log.warn("Error closing connection: {}", connectionId, e);
                failedCleanupCount.incrementAndGet();
            }
        });

        connections.clear();
        connectionTimes.clear();

        log.info("All connections closed, processed connection count: {}", closedCount.get());
    }

    private void removeConnection(String connectionId) {
        log.debug("Start removing connection: {}", connectionId);

        SseEmitter removedEmitter = connections.remove(connectionId);
        Long removedTime = connectionTimes.remove(connectionId);

        // Record removal operation log
        if (removedEmitter != null || removedTime != null) {
            log.info("Connection removal successful - ID: {}, atomic operation: {}", connectionId, (removedEmitter != null && removedTime != null));
        }

        if (sseProperties.getRedis().isEnabled()) {
            // Enhanced connection unregistration, including retry and monitoring
            unregisterConnectionFromRedisWithRetry(connectionId);
        }
    }

    @Override
    public boolean isConnected(String connectionId) {
        return connections.containsKey(connectionId);
    }

    @Override
    public int getConnectionCount() {
        return connections.size();
    }

    @Override
    public Map<String, Long> getConnectionTimes() {
        return new ConcurrentHashMap<>(connectionTimes);
    }

    @Override
    public int getClusterConnectionCount() {
        if (!sseProperties.getRedis().isEnabled()) {
            return 0;
        }

        try {
            String clusterSetKey = sseProperties.getRedis().getPrefix() + "cluster:connections";
            return redisTemplate.opsForSet().size(clusterSetKey).intValue();
        } catch (Exception e) {
            log.error("Error getting cluster connection count", e);
            return 0;
        }
    }

    private void cleanupExpiredConnections() {
        // Ensure cleanup executor is initialized (may not be called in test environment)
        if (cleanupExecutor == null) {
            synchronized (this) {
                if (cleanupExecutor == null) {
                    cleanupExecutor = createNamedScheduledThreadPool(2, "sse-cleanup");
                    log.info("Initializing cleanup executor, thread count: {}", 2);
                }
            }
        }

        long now = System.currentTimeMillis();
        long maxAge = sseProperties.getCleanup().getConnectionTtl();

        log.debug("Start cleaning expired connections, current time: {}, maximum lifetime: {}ms", now, maxAge);

        final AtomicInteger localCleanupCount = new AtomicInteger(0);
        connectionTimes.entrySet().removeIf(entry -> {
            if (now - entry.getValue() > maxAge) {
                log.info("Cleaning expired local connection: {}, creation time: {}", entry.getKey(), entry.getValue());
                closeConnection(entry.getKey());
                localCleanupCount.incrementAndGet();
                return true;
            }
            return false;
        });

        if (localCleanupCount.get() > 0) {
            log.info("Local expired connection cleanup completed, cleanup count: {}", localCleanupCount.get());
        }

        if (sseProperties.getRedis().isEnabled()) {
            cleanupRedisConnectionsWithRetry();
        }
    }

    private void cleanupRedisConnectionsWithRetry() {
        // Check if Redis atomic operations and expired connection cleanup atomicity guarantees are enabled
        if (!sseProperties.getAtomicOperation().isRedisAtomicEnabled()
            || !sseProperties.getAtomicOperation().isExpiredConnectionCleanupAtomic()) {
            fallbackCleanupRedisConnectionsWithRetry();
            return;
        }

        String nodeName = getNodeName();
        int connectionTtl = sseProperties.getRedis().getConnectionTtl();
        long maxAge = connectionTtl * 1000L;
        long currentTime = System.currentTimeMillis();
        int maxRetries = sseProperties.getReliability().getCleanupRetryCount();
        int retryInterval = sseProperties.getReliability().getCleanupRetryInterval();

        // Record retry attempt count
        retryAttemptCount.incrementAndGet();

        // Use unified retry mechanism to execute Lua script
        boolean success = executeWithRetry(() -> {
            Long removedCount = luaScriptService.executeScript(LuaScript.CLEANUP_EXPIRED_CONNECTIONS.name(),
                new ArrayList<>(), Arrays.asList(sseProperties.getRedis().getPrefix(), nodeName, String.valueOf(maxAge),
                    String.valueOf(currentTime)),
                ReturnType.INTEGER);

            if (removedCount != null) {
                log.info("Atomic cleanup of expired connections completed, cleanup count: {}", removedCount);
                successfulCleanupCount.addAndGet(removedCount.intValue());
            } else {
                log.warn("Atomic cleanup of expired connections returned null result");
                throw new RuntimeException("Atomic cleanup of expired connections returned null result");
            }
        }, maxRetries, retryInterval, "atomic cleanup of expired connections");

        if (!success) {
            // All retries failed, use the original cleanup method
            fallbackCleanupRedisConnectionsWithRetry();
        }
    }

    private void fallbackCleanupRedisConnectionsWithRetry() {
        int maxRetries = sseProperties.getReliability().getCleanupRetryCount();
        int retryInterval = sseProperties.getReliability().getCleanupRetryInterval();

        // Record retry attempt count
        retryAttemptCount.incrementAndGet();

        // Use unified retry mechanism to execute cleanup operation
        boolean success = executeWithRetry(() -> {
            // Clean up expired connection key-value pairs - Use SCAN instead of KEYS to avoid blocking
            String pattern = sseProperties.getRedis().getPrefix() + "connections:*";
            int cleanupCount = scanAndCleanupExpiredConnections(pattern);
            log.info("Fallback Redis connection cleanup completed, cleanup count: {}", cleanupCount);
            successfulCleanupCount.addAndGet(cleanupCount);
        }, maxRetries, retryInterval, "fallback Redis connection cleanup");

        if (!success) {
            log.error("Fallback Redis connection cleanup finally failed");
        }
    }

    /**
     * Use SCAN command to scan and clean up expired connection key-value pairs
     * 
     * @param pattern Key pattern
     * @return Number of cleaned connections
     */
    private int scanAndCleanupExpiredConnections(String pattern) {
        int cleanupCount = 0;
        try {
            // Use RedisCallback to execute SCAN operation to avoid blocking
            Set<String> keys = redisTemplate.execute(new RedisCallback<Set<String>>() {
                @Override
                public Set<String> doInRedis(RedisConnection connection) throws DataAccessException {
                    Set<String> result = new HashSet<>();
                    Cursor<byte[]> cursor =
                        connection.scan(ScanOptions.scanOptions().match(pattern).count(100).build());
                    try {
                        while (cursor.hasNext()) {
                            result.add(new String(cursor.next()));
                        }
                    } finally {
                        try {
                            cursor.close();
                        } catch (Exception e) {
                            log.warn("Error closing SCAN cursor", e);
                        }
                    }
                    return result;
                }
            });

            if (keys != null) {
                for (String key : keys) {
                    try {
                        Object value = redisTemplate.opsForValue().get(key);
                        if (value != null) {
                            String[] parts = value.toString().split(":");
                            if (parts.length == 2) {
                                long timestamp = Long.parseLong(parts[1]);
                                long maxAge = sseProperties.getRedis().getConnectionTtl() * 1000L;
                                if (System.currentTimeMillis() - timestamp > maxAge) {
                                    redisTemplate.delete(key);
                                    // Remove from set
                                    String connectionId =
                                        key.substring((sseProperties.getRedis().getPrefix() + "connections:").length());
                                    String clusterSetKey = sseProperties.getRedis().getPrefix() + "cluster:connections";
                                    String nodeSetKey =
                                        sseProperties.getRedis().getPrefix() + "nodes:" + parts[0] + ":connections";
                                    redisTemplate.opsForSet().remove(clusterSetKey, connectionId);
                                    redisTemplate.opsForSet().remove(nodeSetKey, connectionId);
                                    // Remove from route
                                    String routeKey = sseProperties.getRedis().getPrefix() + "routes:" + connectionId;
                                    redisTemplate.delete(routeKey);
                                    cleanupCount++;
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Error processing single connection key: {}", key, e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error cleaning connections with SCAN", e);
        }
        return cleanupCount;
    }

    /**
     * Clean up connection information for this node (used at startup and shutdown)
     */
    private void cleanupNodeConnections() {
        try {
            String nodeName = getNodeName();
            String nodeSetKey = sseProperties.getRedis().getPrefix() + "nodes:" + nodeName + ":connections";
            // First check if the set exists and is not empty
            Long setSize = redisTemplate.opsForSet().size(nodeSetKey);
            if (setSize != null && setSize > 0) {
                // Get all members and delete
                Set<Object> members = redisTemplate.opsForSet().members(nodeSetKey);
                if (members != null && !members.isEmpty()) {
                    Object[] memberArray = members.toArray();
                    Long removedCount = redisTemplate.opsForSet().remove(nodeSetKey, memberArray);
                    log.info("Cleaning this node's connection information completed: {}, node: {}, cleaned connection count: {}", nodeSetKey, nodeName, removedCount);
                } else {
                    log.info("This node's connection information is already empty: {}, node: {}", nodeSetKey, nodeName);
                }
            } else {
                log.info("This node's connection information does not exist or is empty: {}, node: {}", nodeSetKey, nodeName);
            }
        } catch (Exception e) {
            log.error("Error cleaning this node's connection information", e);
            failedCleanupCount.incrementAndGet();
        }
    }

    /**
     * Register route information
     * 
     * @param connectionId Connection ID
     */
    private void registerRoute(String connectionId) {
        String routeKey = sseProperties.getRedis().getPrefix() + "routes:" + connectionId;
        String nodeName = getNodeName();
        redisTemplate.opsForValue().set(routeKey, nodeName, sseProperties.getRedis().getConnectionTtl(),
            TimeUnit.SECONDS);
        log.debug("Register connection route: {} -> {}", connectionId, nodeName);
    }

    /**
     * Unregister route information
     * 
     * @param connectionId Connection ID
     */
    private void unregisterRoute(String connectionId) {
        String routeKey = sseProperties.getRedis().getPrefix() + "routes:" + connectionId;
        redisTemplate.delete(routeKey);
        log.debug("Unregister connection route: {}", connectionId);
    }

    /**
     * Query route information
     * 
     * @param connectionId Connection ID
     * @return Node name, or null if not exists
     */
    public String getRoute(String connectionId) {
        try {
            String routeKey = sseProperties.getRedis().getPrefix() + "routes:" + connectionId;
            Object nodeName = redisTemplate.opsForValue().get(routeKey);
            log.debug("Query connection route: {} -> {}", connectionId, nodeName);
            return nodeName != null ? nodeName.toString() : null;
        } catch (Exception e) {
            log.error("Failed to query connection route information: {}", connectionId, e);
            return null;
        }
    }

    /**
     * Delete route information
     * 
     * @param connectionId Connection ID
     */
    public void removeRoute(String connectionId) {
        unregisterRoute(connectionId);
    }

    /**
     * Handle control command
     * 
     * @param command Command content
     */
    private void handleCommand(String command) {
        log.debug("Received control command: {}", command);
        try {
            // Parse command format: COMMAND:connectionId:data
            String[] parts = command.split(":", 3);
            if (parts.length < 2) {
                log.warn("Invalid control command format: {}", command);
                return;
            }

            String commandType = parts[0];
            String connectionId = parts[1];

            switch (commandType) {
                case "CLOSE_CONNECTION":
                    // Directly close local connection
                    SseEmitter emitter = connections.get(connectionId);
                    if (emitter != null) {
                        try {
                            emitter.complete();
                        } catch (Exception e) {
                            log.error("Error closing connection: {}", connectionId, e);
                        }
                        removeConnection(connectionId);
                    }
                    break;
                case "SEND_MESSAGE":
                    if (parts.length >= 3) {
                        // Re-parse, correctly split SEND_MESSAGE command
                        int firstColon = command.indexOf(':');
                        int secondColon = command.indexOf(':', firstColon + 1);
                        if (secondColon > 0) {
                            String eventName = command.substring(firstColon + 1, secondColon);
                            String data = command.substring(secondColon + 1);
                            sendMessage(connectionId, eventName, data);
                        } else {
                            log.warn("SEND_MESSAGE command parameters insufficient: {}", command);
                        }
                    } else {
                        log.warn("SEND_MESSAGE command parameters insufficient: {}", command);
                    }
                    break;
                default:
                    log.warn("Unknown control command: {}", commandType);
            }
        } catch (Exception e) {
            log.error("Error processing control command: {}", command, e);
        }
    }

    @Override
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Send control command to specified node
     * 
     * @param nodeName Target node name
     * @param command Command content
     */
    private void sendCommand(String nodeName, String command) {
        try {
            String commandChannel = sseProperties.getRedis().getPrefix() + "commands:" + nodeName;
            redisTemplate.convertAndSend(commandChannel, command);
            log.debug("Send control command to node {}: {}", nodeName, command);
        } catch (Exception e) {
            log.error("Error sending control command to node {}: {}", nodeName, command, e);
        }
    }

    /**
     * Record resource recycling statistics
     */
    private void logResourceCleanupStats() {
        log.info("Resource recycling statistics - Success: {}, Failure: {}, Retry attempts: {}", successfulCleanupCount.get(), failedCleanupCount.get(),
            retryAttemptCount.get());
    }

    /**
     * Ensure executor is initialized
     */
    private void ensureExecutorsInitialized() {
        if (cleanupExecutor == null || heartbeatExecutor == null) {
            synchronized (this) {
                if (cleanupExecutor == null) {
                    cleanupExecutor = createNamedScheduledThreadPool(2, "sse-cleanup");
                    log.info("Ensure cleanup executor is initialized");
                }
                if (heartbeatExecutor == null) {
                    int heartbeatThreads =
                        Math.max(2, Math.min(10, sseProperties.getConnection().getMaxConnections() / 50));
                    heartbeatExecutor = createNamedScheduledThreadPool(heartbeatThreads, "sse-heartbeat");
                    log.info("Ensure heartbeat executor is initialized, thread count: {}", heartbeatThreads);
                }
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Start executing SSE service shutdown process, current connection count: {}", connections.size());
        shuttingDown.set(true);

        // Record connection count before shutdown
        int connectionCount = connections.size();
        log.info("Connection count before shutdown: {}", connectionCount);

        // Shutdown thread pools
        try {
            if (sseProperties.getReliability().isGracefulShutdown()) {
                // Send shutdown notification message to all connections
                try {
                    broadcast("system", "server_shutdown");
                    log.info("Server shutdown notification sent");
                } catch (Exception e) {
                    log.warn("Error sending shutdown notification", e);
                }

                // Wait for a while to let connections close normally
                log.info("Waiting for connections to close normally, timeout: {}ms", sseProperties.getReliability().getGracefulShutdownTimeout());
                cleanupExecutor.shutdown();
                heartbeatExecutor.shutdown();

                if (!cleanupExecutor.awaitTermination(sseProperties.getReliability().getGracefulShutdownTimeout(),
                    TimeUnit.MILLISECONDS)) {
                    log.warn("Cleanup executor shutdown timeout, force close all tasks");
                    List<Runnable> cleanupTasks = cleanupExecutor.shutdownNow();
                    log.info("Cleanup executor force closed, remaining task count: {}", cleanupTasks.size());
                }

                if (!heartbeatExecutor.awaitTermination(sseProperties.getReliability().getGracefulShutdownTimeout(),
                    TimeUnit.MILLISECONDS)) {
                    log.warn("Heartbeat executor shutdown timeout, force close all tasks");
                    List<Runnable> heartbeatTasks = heartbeatExecutor.shutdownNow();
                    log.info("Heartbeat executor force closed, remaining task count: {}", heartbeatTasks.size());
                }
            } else {
                log.info("Execute fast shutdown");
                List<Runnable> cleanupTasks = cleanupExecutor.shutdownNow();
                List<Runnable> heartbeatTasks = heartbeatExecutor.shutdownNow();
                log.info("Fast shutdown completed, cleanup executor remaining task count: {}, heartbeat executor remaining task count: {}", cleanupTasks.size(), heartbeatTasks.size());
            }
        } catch (Exception e) {
            log.error("Error occurred while shutting down thread pools", e);
            try {
                List<Runnable> cleanupTasks = cleanupExecutor.shutdownNow();
                List<Runnable> heartbeatTasks = heartbeatExecutor.shutdownNow();
                log.info("Emergency shutdown thread pools, cleanup executor remaining task count: {}, heartbeat executor remaining task count: {}", cleanupTasks.size(), heartbeatTasks.size());
            } catch (Exception shutdownException) {
                log.error("Error occurred during emergency shutdown of thread pools", shutdownException);
            }
        }

        // Close all SSE connections
        try {
            closeAllConnections();
            log.info("All SSE connections closed");
        } catch (Exception e) {
            log.error("Error occurred while closing SSE connections", e);
        }

        // Clean up connection information for this node at shutdown
        try {
            cleanupNodeConnections();
            log.info("This node's connection information has been cleaned up");
        } catch (Exception e) {
            log.error("Error occurred while cleaning up this node's connection information", e);
        }

        // Output resource recycling statistics
        logResourceCleanupStats();

        log.info("SSE service shutdown completed, statistics - Successful cleanup: {}, Failed cleanup: {}, Retry attempts: {}", successfulCleanupCount.get(),
            failedCleanupCount.get(), retryAttemptCount.get());
    }

    /**
     * Unified retry execution mechanism
     * 
     * @param operation Operation to execute
     * @param maxRetries Maximum retry count
     * @param retryInterval Retry interval (milliseconds)
     * @param operationName Operation name (for logging)
     * @return Whether execution was successful
     */
    private boolean executeWithRetry(RetryableOperation operation, int maxRetries, int retryInterval,
        String operationName) {
        // Defensive limit to avoid infinite retries and log explosion caused by extreme values like Integer.MAX_VALUE
        final int effectiveMaxRetries = Math.max(0, Math.min(maxRetries, MAX_ALLOWED_RETRIES));
        for (int attempt = 0; attempt <= effectiveMaxRetries; attempt++) {
            try {
                operation.execute();
                if (attempt > 0) {
                    long totalAttempts = (long)effectiveMaxRetries + 1L; // Use long to avoid overflow
                    log.info("{} execution successful, retry count: {}/{}", operationName, attempt, totalAttempts);
                }
                // Success, return directly
                return true;
            } catch (Exception e) {
                if (attempt < effectiveMaxRetries) {
                    long nextAttempt = (long)attempt + 1L;
                    long totalAttempts = (long)effectiveMaxRetries + 1L;
                    boolean logWarn = attempt < RETRY_LOG_VERBOSE_HEAD
                        || (attempt % RETRY_LOG_STEP == (RETRY_LOG_STEP - 1));
                    if (logWarn) {
                        log.warn("{} execution failed, attempt count: {}/{}, operation context: [is shutting down: {}]", operationName, nextAttempt, totalAttempts,
                            shuttingDown.get(), e);
                    } else if (log.isDebugEnabled()) {
                        log.debug("{} execution failed (log suppressed), attempt count: {}/{}", operationName, nextAttempt, totalAttempts);
                    }
                    try {
                        // Check if shutting down, reduce wait time
                        int effectiveWaitTime = shuttingDown.get() ? Math.min(retryInterval, 100) : retryInterval;
                        Thread.sleep(effectiveWaitTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.warn("{} retry interrupted", operationName);
                        return false;
                    }
                } else {
                    // Last attempt still failed
                    long totalAttempts = (long)effectiveMaxRetries + 1L;
                    log.error("{} final execution failed, retried {} times, operation context: [is shutting down: {}]", operationName, totalAttempts, shuttingDown.get(),
                        e);
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * Retryable operation interface
     */
    @FunctionalInterface
    public interface RetryableOperation {
        void execute() throws Exception;
    }
}