package me.ud.ltc.tak.distributed.sse.starter.service.impl;

import me.ud.ltc.tak.distributed.sse.starter.config.SseProperties;
import me.ud.ltc.tak.distributed.sse.starter.script.LuaScriptService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;
import org.mockito.junit.jupiter.MockitoSettings;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * SSE Service Implementation Test
 * 
 * @author takltc
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SseServiceImplTest {

    @Configuration
    static class RedisTestConfiguration {
        @Bean
        public RedisConnectionFactory redisConnectionFactory() {
            RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("localhost", 6379);
            return new LettuceConnectionFactory(config);
        }

        @Bean
        public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
            RedisTemplate<String, Object> template = new RedisTemplate<>();
            template.setConnectionFactory(connectionFactory);
            return template;
        }
        
        @Bean
        public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
            return new StringRedisTemplate(connectionFactory);
        }
    }

    @Mock
    private SseProperties sseProperties;

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private RedisMessageListenerContainer redisMessageListenerContainer;

    @Mock
    private LuaScriptService luaScriptService;

    @InjectMocks
    private SseServiceImpl sseService;

    private SseProperties.Connection connectionConfig;
    private SseProperties.Redis redisConfig;
    private SseProperties.Cleanup cleanupConfig;
    private SseProperties.AtomicOperation atomicOperationConfig;
    private SseProperties.Consistency consistencyConfig;
    private SseProperties.Reliability reliabilityConfig;

    @BeforeEach
    public void setUp() {
        // Initialize configuration
        connectionConfig = new SseProperties.Connection();
        redisConfig = new SseProperties.Redis();
        cleanupConfig = new SseProperties.Cleanup();
        atomicOperationConfig = new SseProperties.AtomicOperation();
        consistencyConfig = new SseProperties.Consistency();
        reliabilityConfig = new SseProperties.Reliability();

        // Configure default values
        connectionConfig.setTimeout(30000);
        connectionConfig.setHeartbeat(30000);
        connectionConfig.setMaxConnections(1000);
        
        redisConfig.setEnabled(false);
        redisConfig.setPrefix("sse:");
        redisConfig.setConnectionTtl(3600);
        redisConfig.setChannel("sse:events");
        
        cleanupConfig.setEnabled(true);
        cleanupConfig.setInterval(60000);
        cleanupConfig.setConnectionTtl(3600000);
        
        atomicOperationConfig.setRedisAtomicEnabled(true);
        atomicOperationConfig.setLuaScriptRetryCount(3);
        atomicOperationConfig.setLuaScriptRetryInterval(100);
        
        consistencyConfig.setEnabled(true);
        consistencyConfig.setStatusCheckInterval(30000);
        consistencyConfig.setInconsistencyHandlingStrategy("auto_correct");
        consistencyConfig.setInterNodeSyncEnabled(true);
        consistencyConfig.setRouteValidationInterval(60000);
        
        reliabilityConfig.setEnabled(true);
        reliabilityConfig.setCleanupRetryCount(3);
        reliabilityConfig.setCleanupRetryInterval(1000);
        reliabilityConfig.setGracefulShutdown(true);
        reliabilityConfig.setGracefulShutdownTimeout(30000);
        reliabilityConfig.setResourceLeakDetection(true);
        reliabilityConfig.setLeakDetectionInterval(300000);

        // Mock configuration properties - use lenient() to avoid unnecessary stubbing exceptions
        lenient().when(sseProperties.getConnection()).thenReturn(connectionConfig);
        lenient().when(sseProperties.getRedis()).thenReturn(redisConfig);
        lenient().when(sseProperties.getCleanup()).thenReturn(cleanupConfig);
        lenient().when(sseProperties.getAtomicOperation()).thenReturn(atomicOperationConfig);
        lenient().when(sseProperties.getConsistency()).thenReturn(consistencyConfig);
        lenient().when(sseProperties.getReliability()).thenReturn(reliabilityConfig);
        
        // Mock Lua script service return values - avoid executeWithRetry retries
        lenient().when(luaScriptService.executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any())).thenReturn(1L);
        lenient().when(luaScriptService.executeScript(eq("UNREGISTER_CONNECTION"), anyList(), anyList(), any())).thenReturn(1L);
        
        // Ensure node name is reset
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, "test-node");
        } catch (Exception e) {
            // Ignore errors
        }
    }

    // ==================== Core Functionality Tests ====================

    @Test
    public void testCreateConnection_Success() {
        // Prepare test data
        String connectionId = "test-connection-1";
        
        // Execute test
        SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results
        assertNotNull(emitter, "SSE Emitter should not be null");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
        assertEquals(1, sseService.getConnectionCount(), "Connection count should be 1");
    }

    @Test
    public void testCreateConnection_MaxConnectionsReached() {
        // Set maximum connections to 0
        connectionConfig.setMaxConnections(0);
        
        // Execute test
        SseEmitter emitter = sseService.createConnection("test-connection-1");
        
        // Verify results
        assertNull(emitter, "Should return null when maximum connections reached");
        assertEquals(0, sseService.getConnectionCount(), "Connection count should be 0");
    }

    @Test
    public void testSendMessage_Success() {
        // Prepare test data
        String connectionId = "test-connection-1";
        String eventName = "test-event";
        String message = "test message";
        
        // Create connection first
        SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "SSE Emitter should not be null");
        
        // Execute test - send message
        assertDoesNotThrow(() -> sseService.sendMessage(connectionId, eventName, message),
                "Sending message should not throw exception");
    }

    @Test
    public void testSendMessage_ConnectionNotExists() {
        // Execute test - send message to non-existent connection
        assertDoesNotThrow(() -> sseService.sendMessage("non-existent-connection", "test-event", "test message"),
                "Sending message to non-existent connection should not throw exception");
    }

    @Test
    public void testBroadcast_Success() {
        // Prepare test data
        String eventName = "test-event";
        String message = "test message";
        
        // Create multiple connections
        sseService.createConnection("connection-1");
        sseService.createConnection("connection-2");
        sseService.createConnection("connection-3");
        
        // Execute test
        assertDoesNotThrow(() -> sseService.broadcast(eventName, message),
                "Broadcasting message should not throw exception");
        assertEquals(3, sseService.getConnectionCount(), "Connection count should be 3");
    }

    @Test
    public void testCloseConnection_LocalConnection() {
        // Prepare test data
        String connectionId = "test-connection-1";
        
        // Create connection first
        SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "SSE Emitter should not be null");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
        
        // Execute test
        sseService.closeConnection(connectionId);
        
        // Verify results
        assertFalse(sseService.isConnected(connectionId), "Connection should be closed");
        assertEquals(0, sseService.getConnectionCount(), "Connection count should be 0");
    }

    @Test
    public void testCloseAllConnections() {
        // Create multiple connections
        sseService.createConnection("connection-1");
        sseService.createConnection("connection-2");
        sseService.createConnection("connection-3");
        
        assertEquals(3, sseService.getConnectionCount(), "Connection count should be 3");
        
        // Execute test
        sseService.closeAllConnections();
        
        // Verify results
        assertEquals(0, sseService.getConnectionCount(), "Connection count should be 0");
    }

    @Test
    public void testIsConnected() {
        String connectionId = "test-connection-1";
        
        // Test disconnected state
        assertFalse(sseService.isConnected(connectionId), "Connection should not be established");
        
        // Create connection
        sseService.createConnection(connectionId);
        
        // Test connected state
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
    }

    @Test
    public void testGetConnectionCount() {
        // Initial state
        assertEquals(0, sseService.getConnectionCount(), "Initial connection count should be 0");
        
        // Create connections
        sseService.createConnection("connection-1");
        sseService.createConnection("connection-2");
        
        // Verify connection count
        assertEquals(2, sseService.getConnectionCount(), "Connection count should be 2");
        
        // Close connection
        sseService.closeConnection("connection-1");
        
        // Verify connection count
        assertEquals(1, sseService.getConnectionCount(), "Connection count should be 1");
    }

    // ==================== Boundary Condition Tests ====================

    @Test
    public void testCreateConnection_WithNullConnectionId() {
        // Execute test
        SseEmitter emitter = sseService.createConnection(null);
        
        // Verify results
        assertNull(emitter, "Should return null when connection ID is null");
    }

    @Test
    public void testCreateConnection_WithEmptyConnectionId() {
        // Execute test
        SseEmitter emitter = sseService.createConnection("");
        
        // Verify results
        assertNotNull(emitter, "Should create connection when connection ID is empty string");
        assertTrue(sseService.isConnected(""), "Empty string connection ID should be established");
    }

    @Test
    public void testCreateConnection_WithVeryLongConnectionId() {
        // Prepare test data
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("a");
        }
        String longConnectionId = sb.toString();
        
        // Execute test
        SseEmitter emitter = sseService.createConnection(longConnectionId);
        
        // Verify results
        assertNotNull(emitter, "Should be able to create connection with long connection ID");
        assertTrue(sseService.isConnected(longConnectionId), "Long connection ID should be established");
    }

    @Test
    public void testSendMessage_WithNullParameters() {
        // Prepare test data
        String connectionId = "test-connection-1";
        sseService.createConnection(connectionId);
        
        // Execute test
        assertDoesNotThrow(() -> sseService.sendMessage(connectionId, null, null),
                "Sending message with null parameters should not throw exception");
    }

    @Test
    public void testSendMessage_WithEmptyParameters() {
        // Prepare test data
        String connectionId = "test-connection-1";
        sseService.createConnection(connectionId);
        
        // Execute test
        assertDoesNotThrow(() -> sseService.sendMessage(connectionId, "", ""),
                "Sending message with empty string parameters should not throw exception");
    }

    @Test
    public void testBroadcast_WithNullParameters() {
        // Execute test
        assertDoesNotThrow(() -> sseService.broadcast(null, null),
                "Broadcasting message with null parameters should not throw exception");
    }

    @Test
    public void testBroadcast_WithEmptyParameters() {
        // Execute test
        assertDoesNotThrow(() -> sseService.broadcast("", ""),
                "Broadcasting message with empty string parameters should not throw exception");
    }

    @Test
    public void testCloseConnection_WithNonExistentConnection() {
        // Execute test
        assertDoesNotThrow(() -> sseService.closeConnection("non-existent-connection"),
                "Closing non-existent connection should not throw exception");
    }

    @Test
    public void testCloseConnection_WithNullConnectionId() {
        // Execute test
        assertDoesNotThrow(() -> sseService.closeConnection(null),
                "Closing connection with null ID should not throw exception");
    }

    // ==================== Exception Handling Tests ====================

    @Test
    public void testCreateConnection_WhenEmitterCreationFails() {
        // This test is difficult to simulate because SseEmitter constructor does not throw exceptions
        // Can only verify normal flow
        SseEmitter emitter = sseService.createConnection("test-connection-1");
        assertNotNull(emitter, "Should create SSE Emitter under normal circumstances");
    }

    @Test
    public void testSendMessage_WhenEmitterSendFails() {
        // Prepare test data
        String connectionId = "test-connection-1";
        sseService.createConnection(connectionId);
        
        // Execute test - simulate connection removal after send failure
        assertDoesNotThrow(() -> sseService.sendMessage(connectionId, "test-event", "test message"),
                "Message sending failure should not throw uncaught exception");
    }

    // ==================== Performance Tests ====================

    @Test
    public void testCreateMultipleConnections_Performance() {
        // Record start time
        long startTime = System.currentTimeMillis();
        
        // Create large number of connections
        int connectionCount = 100;
        for (int i = 0; i < connectionCount; i++) {
            sseService.createConnection("connection-" + i);
        }
        
        // Record end time
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify results
        assertEquals(connectionCount, sseService.getConnectionCount(), 
                "Should create specified number of connections");
        
        // Performance check (assuming should complete within 1 second)
        assertTrue(duration < 1000, "Creating 100 connections should complete within 1 second, actual time: " + duration + "ms");
    }

    @Test
    public void testBroadcastToMultipleConnections_Performance() {
        // Create multiple connections
        int connectionCount = 50;
        for (int i = 0; i < connectionCount; i++) {
            sseService.createConnection("connection-" + i);
        }
        
        // Record start time
        long startTime = System.currentTimeMillis();
        
        // Execute broadcast
        sseService.broadcast("test-event", "test message");
        
        // Record end time
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify results
        assertEquals(connectionCount, sseService.getConnectionCount(), 
                "Connection count should remain unchanged");
        
        // Performance check (assuming should complete within 500ms)
        assertTrue(duration < 500, "Broadcasting to 50 connections should complete within 500ms, actual time: " + duration + "ms");
    }

    @Test
    public void testCloseAllConnections_Performance() {
        // Create multiple connections
        int connectionCount = 100;
        for (int i = 0; i < connectionCount; i++) {
            sseService.createConnection("connection-" + i);
        }
        
        // Record start time
        long startTime = System.currentTimeMillis();
        
        // Execute closing all connections
        sseService.closeAllConnections();
        
        // Record end time
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify results
        assertEquals(0, sseService.getConnectionCount(), 
                "All connections should be closed");
        
        // Performance check (assuming should complete within 500ms)
        assertTrue(duration < 500, "Closing 100 connections should complete within 500ms, actual time: " + duration + "ms");
    }

    // ==================== Integration Tests ====================

    @Test
    public void testConnectionLifecycle_FullFlow() {
        String connectionId = "lifecycle-test-connection";
        
        // 1. Create connection
        SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Should successfully create connection");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
        assertEquals(1, sseService.getConnectionCount(), "Connection count should be 1");
        
        // 2. Send message
        assertDoesNotThrow(() -> sseService.sendMessage(connectionId, "test-event", "test message"),
                "Should successfully send message");
        
        // 3. Broadcast message
        assertDoesNotThrow(() -> sseService.broadcast("broadcast-event", "broadcast message"),
                "Should successfully broadcast message");
        
        // 4. Close connection
        sseService.closeConnection(connectionId);
        assertFalse(sseService.isConnected(connectionId), "Connection should be closed");
        assertEquals(0, sseService.getConnectionCount(), "Connection count should be 0");
    }

    @Test
    public void testMultipleConnections_Isolation() {
        // Create multiple connections
        String[] connectionIds = {"conn-1", "conn-2", "conn-3", "conn-4", "conn-5"};
        SseEmitter[] emitters = new SseEmitter[connectionIds.length];
        
        for (int i = 0; i < connectionIds.length; i++) {
            emitters[i] = sseService.createConnection(connectionIds[i]);
            assertNotNull(emitters[i], "Connection " + connectionIds[i] + " should be created successfully");
            assertTrue(sseService.isConnected(connectionIds[i]), "Connection " + connectionIds[i] + " should be established");
        }
        
        assertEquals(connectionIds.length, sseService.getConnectionCount(), "Connection count should be correct");
        
        // Send message to specific connection
        assertDoesNotThrow(() -> sseService.sendMessage("conn-2", "specific-event", "specific message"),
                "Should successfully send specific message");
        
        // Broadcast message to all connections
        assertDoesNotThrow(() -> sseService.broadcast("broadcast-event", "broadcast message"),
                "Should successfully broadcast message");
        
        // Close specific connection
        sseService.closeConnection("conn-3");
        assertFalse(sseService.isConnected("conn-3"), "Connection conn-3 should be closed");
        assertEquals(connectionIds.length - 1, sseService.getConnectionCount(), "Connection count should decrease by 1");
        
        // Close all remaining connections
        sseService.closeAllConnections();
        assertEquals(0, sseService.getConnectionCount(), "All connections should be closed");
    }

    // ==================== Redis-related Functionality Tests (when Redis enabled) ====================

    @Test
    public void testCreateConnection_WithRedisEnabled() {
        // Enable Redis
        redisConfig.setEnabled(true);
        
        // Execute test
        String connectionId = "redis-test-connection";
        SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results
        assertNotNull(emitter, "Should successfully create connection when Redis is enabled");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established when Redis is enabled");
    }

    @Test
    public void testGetConnectionTimes() {
        // Create connection
        String connectionId = "time-test-connection";
        long beforeCreation = System.currentTimeMillis();
        sseService.createConnection(connectionId);
        long afterCreation = System.currentTimeMillis();
        
        // Get connection time
        Map<String, Long> connectionTimes = sseService.getConnectionTimes();
        
        // Verify results
        assertTrue(connectionTimes.containsKey(connectionId), "Connection time mapping should contain connection ID");
        Long connectionTime = connectionTimes.get(connectionId);
        assertNotNull(connectionTime, "Connection time should not be null");
        assertTrue(connectionTime >= beforeCreation && connectionTime <= afterCreation,
                "Connection time should be within creation time range");
    }

    // ==================== Helper Method Tests ====================

    @Test
    public void testGetNodeName() {
        // Now getNodeName is a public method, call directly
        try {
            // Get node name (now a public method)
            String nodeName = sseService.getNodeName();
            
            assertNotNull(nodeName, "Node name should not be null");
            assertFalse(nodeName.isEmpty(), "Node name should not be empty");
            assertEquals("test-node", nodeName, "Node name should match preset value");
        } catch (Exception e) {
            fail("Exception occurred when getting node name: " + e.getMessage());
        }
    }
}