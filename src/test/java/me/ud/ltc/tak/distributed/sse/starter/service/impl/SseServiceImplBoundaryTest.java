package me.ud.ltc.tak.distributed.sse.starter.service.impl;

import me.ud.ltc.tak.distributed.sse.starter.config.SseProperties;
import me.ud.ltc.tak.distributed.sse.starter.script.LuaScriptService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Boundary condition tests for SSE service implementation
 * 
 * @author takltc
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SseServiceImplBoundaryTest {

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

        // Configure boundary values
        connectionConfig.setTimeout(1); // Minimum timeout
        connectionConfig.setHeartbeat(1000); // Minimum heartbeat interval
        connectionConfig.setMaxConnections(1); // Minimum connection limit
        
        redisConfig.setEnabled(false);
        redisConfig.setPrefix("");
        redisConfig.setConnectionTtl(1);
        redisConfig.setChannel("");
        
        cleanupConfig.setEnabled(true);
        cleanupConfig.setInterval(1000);
        cleanupConfig.setConnectionTtl(1000);
        
        atomicOperationConfig.setRedisAtomicEnabled(false);
        atomicOperationConfig.setLuaScriptRetryCount(0);
        atomicOperationConfig.setLuaScriptRetryInterval(0);
        
        consistencyConfig.setEnabled(false);
        consistencyConfig.setStatusCheckInterval(1000);
        consistencyConfig.setInconsistencyHandlingStrategy("fail_fast");
        
        reliabilityConfig.setEnabled(false);
        reliabilityConfig.setCleanupRetryCount(0);
        reliabilityConfig.setCleanupRetryInterval(0);
        reliabilityConfig.setGracefulShutdown(false);
        reliabilityConfig.setGracefulShutdownTimeout(1);
        reliabilityConfig.setResourceLeakDetection(false);
        reliabilityConfig.setLeakDetectionInterval(1000);

        // Mock configuration properties
        when(sseProperties.getConnection()).thenReturn(connectionConfig);
        when(sseProperties.getRedis()).thenReturn(redisConfig);
        when(sseProperties.getCleanup()).thenReturn(cleanupConfig);
        when(sseProperties.getAtomicOperation()).thenReturn(atomicOperationConfig);
        when(sseProperties.getConsistency()).thenReturn(consistencyConfig);
        when(sseProperties.getReliability()).thenReturn(reliabilityConfig);
    }

    @Test
    public void testCreateConnection_WithMinimumTimeout() {
        String connectionId = "min-timeout-test";
        
        // Execute test
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results
        assertNotNull(emitter, "Connection should be created successfully");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
    }

    @Test
    public void testCreateConnection_AtMaximumLimit() {
        String connectionId = "max-limit-test";
        
        // Create a connection (reaching maximum limit)
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter1 = sseService.createConnection(connectionId);
        assertNotNull(emitter1, "First connection should be created successfully");
        
        // Try to create second connection (should fail)
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter2 = sseService.createConnection("second-connection");
        assertNull(emitter2, "Should return null when exceeding maximum connection limit");
        
        // Verify connection count
        assertEquals(1, sseService.getConnectionCount(), "Connection count should be 1");
    }

    @Test
    public void testCreateConnection_WithZeroMaxConnections() {
        // Set maximum connections to 0
        connectionConfig.setMaxConnections(0);
        
        // Execute test
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection("zero-limit-test");
        
        // Verify results
        assertNull(emitter, "Should return null when maximum connections is 0");
        assertEquals(0, sseService.getConnectionCount(), "Connection count should be 0");
    }

    @Test
    public void testCreateConnection_WithNegativeMaxConnections() {
        // Set maximum connections to negative value
        connectionConfig.setMaxConnections(-1);
        
        // Execute test
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection("negative-limit-test");
        
        // Verify results
        assertNull(emitter, "Should return null when maximum connections is negative");
        assertEquals(0, sseService.getConnectionCount(), "Connection count should be 0");
    }

    @Test
    public void testCreateConnection_WithMaximumMaxConnections() {
        // Set maximum connections to maximum integer value
        connectionConfig.setMaxConnections(Integer.MAX_VALUE);
        
        // Create multiple connections
        String connectionId = "max-value-test";
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results
        assertNotNull(emitter, "Should be able to create connection when maximum connections is Integer.MAX_VALUE");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
    }

    @Test
    public void testSendMessage_WithMaximumMessageSize() {
        String connectionId = "max-message-test";
        
        // Create connection
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        
        // Create large message (approaching JVM limit)
        StringBuilder largeMessage = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            largeMessage.append("a");
        }
        String message = largeMessage.toString();
        
        // Execute test
        assertDoesNotThrow(() -> sseService.sendMessage(connectionId, "large-event", message),
                "Sending large message should not throw exception");
    }

    @Test
    public void testSendMessage_WithEmptyMessage() {
        String connectionId = "empty-message-test";
        
        // Create connection
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        
        // Execute test
        assertDoesNotThrow(() -> sseService.sendMessage(connectionId, "empty-event", ""),
                "Sending empty message should not throw exception");
    }

    @Test
    public void testSendMessage_WithNullMessage() {
        String connectionId = "null-message-test";
        
        // Create connection
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        
        // Execute test
        assertDoesNotThrow(() -> sseService.sendMessage(connectionId, "null-event", null),
                "Sending null message should not throw exception");
    }

    @Test
    public void testBroadcast_WithNoConnections() {
        // Execute test
        assertDoesNotThrow(() -> sseService.broadcast("no-connections-event", "no connections message"),
                "Broadcasting to no connections should not throw exception");
        
        assertEquals(0, sseService.getConnectionCount(), "Connection count should be 0");
    }

    @Test
    public void testBroadcast_WithMaximumConnections() {
        // Set maximum connections
        connectionConfig.setMaxConnections(100);
        
        // Create maximum number of connections
        for (int i = 0; i < 100; i++) {
            org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection("connection-" + i);
            assertNotNull(emitter, "Connection " + i + " should be created successfully");
        }
        
        assertEquals(100, sseService.getConnectionCount(), "Should create 100 connections");
        
        // Execute test
        assertDoesNotThrow(() -> sseService.broadcast("max-connections-event", "maximum connections message"),
                "Broadcasting to maximum connections should not throw exception");
    }

    @Test
    public void testCloseConnection_WithAlreadyClosedConnection() {
        String connectionId = "already-closed-test";
        
        // Create connection
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        
        // First close
        sseService.closeConnection(connectionId);
        assertFalse(sseService.isConnected(connectionId), "Connection should be closed");
        
        // Second close (duplicate close)
        assertDoesNotThrow(() -> sseService.closeConnection(connectionId),
                "Duplicate connection close should not throw exception");
    }

    @Test
    public void testCloseAllConnections_WhenAlreadyClosed() {
        // Create and close connection
        String connectionId = "close-all-test";
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        sseService.closeConnection(connectionId);
        
        // Close all connections again
        assertDoesNotThrow(() -> sseService.closeAllConnections(),
                "Closing already closed connections should not throw exception");
        
        assertEquals(0, sseService.getConnectionCount(), "Connection count should be 0");
    }

    @Test
    public void testGetConnectionCount_WhenNegative() {
        // Use reflection to directly modify connection map to simulate exception condition
        try {
            java.lang.reflect.Field connectionsField = SseServiceImpl.class.getDeclaredField("connections");
            connectionsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.Map<String, org.springframework.web.servlet.mvc.method.annotation.SseEmitter> connections = 
                (java.util.Map<String, org.springframework.web.servlet.mvc.method.annotation.SseEmitter>) connectionsField.get(sseService);
            
            // Clear connection map
            connections.clear();
            
            // Verify connection count
            int count = sseService.getConnectionCount();
            assertTrue(count >= 0, "Connection count should not be negative");
        } catch (Exception e) {
            fail("Exception occurred while accessing connection map: " + e.getMessage());
        }
    }

    @Test
    public void testGetConnectionTimes_Empty() {
        // Get empty connection time map
        java.util.Map<String, Long> connectionTimes = sseService.getConnectionTimes();
        
        // Verify results
        assertNotNull(connectionTimes, "Connection time map should not be null");
        assertTrue(connectionTimes.isEmpty(), "Connection time map should be empty in empty state");
    }

    @Test
    public void testGetConnectionTimes_ConcurrentModification() {
        // Increase maximum connections to avoid default minimum limit causing only 1 connection to be created
        connectionConfig.setMaxConnections(20);

        // Create multiple connections
        for (int i = 0; i < 10; i++) {
            sseService.createConnection("conn-" + i);
        }
        
        // Get connection time map
        java.util.Map<String, Long> connectionTimes = sseService.getConnectionTimes();
        
        // Verify results
        assertNotNull(connectionTimes, "Connection time map should not be null");
        assertEquals(10, connectionTimes.size(), "Connection time map size should be correct");
        
        // Verify content consistency
        for (int i = 0; i < 10; i++) {
            assertTrue(connectionTimes.containsKey("conn-" + i), "Should contain connection conn-" + i);
            assertTrue(connectionTimes.get("conn-" + i) > 0, "Connection time should be greater than 0");
        }
    }

    @Test
    public void testIsConnected_WithSpecialCharacters() {
        String connectionId = "special!@#$%^&*()_+-=[]{}|;':\",./<>?`~ chars";
        
        // Test disconnected state
        assertFalse(sseService.isConnected(connectionId), "Special character connection ID should be disconnected");
        
        // Create connection
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Should successfully create connection with special character connection ID");
        
        // Test connected state
        assertTrue(sseService.isConnected(connectionId), "Special character connection ID should be connected");
        
        // Close connection
        sseService.closeConnection(connectionId);
        assertFalse(sseService.isConnected(connectionId), "Special character connection ID should be closed");
    }

    @Test
    public void testIsConnected_WithUnicodeCharacters() {
        String connectionId = "unicode测试连接中文名称😀🎉🚀";
        
        // Test disconnected state
        assertFalse(sseService.isConnected(connectionId), "Unicode character connection ID should be disconnected");
        
        // Create connection
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Should successfully create connection with Unicode character connection ID");
        
        // Test connected state
        assertTrue(sseService.isConnected(connectionId), "Unicode character connection ID should be connected");
    }

    @Test
    public void testCreateConnection_WithConcurrentSameId() {
        String connectionId = "concurrent-same-id-test";
        
        // Simultaneously create connections with same ID (simulate concurrent scenario)
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter1 = sseService.createConnection(connectionId);
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter2 = sseService.createConnection(connectionId);
        
        // Verify results - second should replace first
        assertNotNull(emitter1, "First connection should be created successfully");
        assertNotNull(emitter2, "Second connection should be created successfully");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
        assertEquals(1, sseService.getConnectionCount(), "Connection count should be 1");
    }

    @Test
    public void testZeroRetryConfiguration() {
        // Configure zero retry count
        atomicOperationConfig.setLuaScriptRetryCount(0);
        reliabilityConfig.setCleanupRetryCount(0);
        
        String connectionId = "zero-retry-test";
        
        // Create connection
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        
        // Close connection
        sseService.closeConnection(connectionId);
        
        // Verify connection is closed
        assertFalse(sseService.isConnected(connectionId), "Connection should be closed");
    }

    @Test
    public void testMaximumRetryConfiguration() {
        // Configure maximum retry count
        atomicOperationConfig.setLuaScriptRetryCount(Integer.MAX_VALUE);
        reliabilityConfig.setCleanupRetryCount(Integer.MAX_VALUE);
        
        String connectionId = "max-retry-test";
        
        // Create connection
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        
        // Verify connection is established
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
    }
}