package me.ud.ltc.tak.distributed.sse.starter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

/**
 * Server-Sent Events configuration properties class
 * 
 * @author takltc
 */
@Data
@Component
@ConfigurationProperties(prefix = "takltc.sse")
public class SseProperties {

    /**
     * Whether to enable SSE functionality
     */
    private boolean enabled = true;

    /**
     * Connection configuration
     */
    private Connection connection = new Connection();

    /**
     * Redis configuration (for cluster support)
     */
    private Redis redis = new Redis();

    /**
     * Cleanup configuration
     */
    private Cleanup cleanup = new Cleanup();

    /**
     * Atomic operation configuration
     */
    private AtomicOperation atomicOperation = new AtomicOperation();

    /**
     * Connection state consistency assurance configuration
     */
    private Consistency consistency = new Consistency();

    /**
     * Resource recycling reliability configuration
     */
    private Reliability reliability = new Reliability();

    @Data
    public static class Connection {
        /**
         * SSE connection timeout (milliseconds)
         */
        private int timeout = 30000;

        /**
         * Heartbeat interval (milliseconds)
         */
        private int heartbeat = 30000;

        /**
         * Maximum number of connections
         */
        private int maxConnections = 1000;
    }

    @Data
    public static class Redis {
        /**
         * Whether to enable Redis support
         */
        private boolean enabled = true;

        /**
         * Redis key prefix
         */
        private String prefix = "sse:";

        /**
         * Connection TTL (seconds)
         */
        private int connectionTtl = 3600;

        /**
         * Broadcast channel name
         */
        private String channel = "sse:events";

        /**
         * Node information TTL (seconds)
         */
        private int nodeTtl = 300;

        /**
         * Redis Lua script execution timeout (milliseconds)
         */
        private int scriptTimeout = 5000;
    }

    @Data
    public static class Cleanup {
        /**
         * Whether to enable cleanup functionality
         */
        private boolean enabled = true;

        /**
         * Cleanup interval (milliseconds)
         */
        private int interval = 60000;

        /**
         * Connection TTL (milliseconds)
         */
        private int connectionTtl = 3600000;
    }

    @Data
    public static class AtomicOperation {
        /**
         * Whether to enable Redis atomic operations
         */
        private boolean redisAtomicEnabled = true;

        /**
         * Lua script retry count
         */
        private int luaScriptRetryCount = 3;

        /**
         * Lua script retry interval (milliseconds)
         */
        private int luaScriptRetryInterval = 100;

        /**
         * Redis operation timeout (milliseconds)
         */
        private int redisOperationTimeout = 3000;

        /**
         * Whether to enable atomicity assurance for connection registration
         */
        private boolean connectionRegistrationAtomic = true;

        /**
         * Whether to enable atomicity assurance for connection unregistration
         */
        private boolean connectionUnregistrationAtomic = true;

        /**
         * Whether to enable atomicity assurance for expired connection cleanup
         */
        private boolean expiredConnectionCleanupAtomic = true;
    }

    @Data
    public static class Consistency {
        /**
         * Whether to enable connection state consistency assurance
         */
        private boolean enabled = true;

        /**
         * Connection state check interval (milliseconds)
         */
        private int statusCheckInterval = 30000;

        /**
         * Connection state synchronization timeout (milliseconds)
         */
        private int statusSyncTimeout = 5000;

        /**
         * Maximum state synchronization retry count
         */
        private int maxStatusSyncRetries = 3;

        /**
         * Handling strategy when inconsistent (fail_fast|auto_correct)
         */
        private String inconsistencyHandlingStrategy = "auto_correct";

        /**
         * Whether to enable inter-node connection state synchronization
         */
        private boolean interNodeSyncEnabled = true;

        /**
         * Connection route information validation interval (milliseconds)
         */
        private int routeValidationInterval = 60000;
    }

    @Data
    public static class Reliability {
        /**
         * Whether to enable resource recycling reliability assurance
         */
        private boolean enabled = true;

        /**
         * Resource recycling failure retry count
         */
        private int cleanupRetryCount = 3;

        /**
         * Resource recycling failure retry interval (milliseconds)
         */
        private int cleanupRetryInterval = 1000;

        /**
         * Cleanup operation timeout (milliseconds)
         */
        private int cleanupTimeout = 10000;

        /**
         * Whether to enable graceful shutdown
         */
        private boolean gracefulShutdown = true;

        /**
         * Graceful shutdown timeout (milliseconds)
         */
        private int gracefulShutdownTimeout = 30000;

        /**
         * Whether to enable resource leak detection
         */
        private boolean resourceLeakDetection = true;

        /**
         * Resource leak detection interval (milliseconds)
         */
        private int leakDetectionInterval = 300000;
    }
}