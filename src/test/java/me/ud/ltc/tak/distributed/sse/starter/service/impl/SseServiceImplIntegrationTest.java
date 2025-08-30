package me.ud.ltc.tak.distributed.sse.starter.service.impl;

import me.ud.ltc.tak.distributed.sse.starter.config.SseProperties;
import me.ud.ltc.tak.distributed.sse.starter.script.LuaScriptService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * SSE Service Implementation Integration Test (Redis-related functionality)
 * 
 * @author takltc
 */
@ExtendWith(MockitoExtension.class)
public class SseServiceImplIntegrationTest {

    @Mock
    private SseProperties sseProperties;

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private RedisMessageListenerContainer redisMessageListenerContainer;

    @Mock
    private LuaScriptService luaScriptService;

    @Mock
    private ValueOperations<String, Object> valueOperations;

    @Mock
    private SetOperations<String, Object> setOperations;

    @Mock
    private RedisConnectionFactory redisConnectionFactory;

    @Mock
    private RedisConnection redisConnection;

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
        
        redisConfig.setEnabled(true); // Enable Redis for integration testing
        redisConfig.setPrefix("sse:");
        redisConfig.setConnectionTtl(3600);
        redisConfig.setChannel("sse:events");
        
        cleanupConfig.setEnabled(true);
        cleanupConfig.setInterval(60000);
        cleanupConfig.setConnectionTtl(3600000);
        
        atomicOperationConfig.setRedisAtomicEnabled(true);
        atomicOperationConfig.setLuaScriptRetryCount(3);
        atomicOperationConfig.setLuaScriptRetryInterval(100);
        atomicOperationConfig.setConnectionRegistrationAtomic(true);
        atomicOperationConfig.setConnectionUnregistrationAtomic(true);
        atomicOperationConfig.setExpiredConnectionCleanupAtomic(true);
        
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

        // Mock Redis operations - use lenient() to avoid unnecessary stubbing exceptions
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(redisTemplate.opsForSet()).thenReturn(setOperations);
        lenient().when(redisTemplate.getConnectionFactory()).thenReturn(redisConnectionFactory);
        lenient().when(redisConnectionFactory.getConnection()).thenReturn(redisConnection);
        
        // Mock Lua script service return values - avoid executeWithRetry retries
        lenient().when(luaScriptService.executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any())).thenReturn(1L);
        lenient().when(luaScriptService.executeScript(eq("UNREGISTER_CONNECTION"), anyList(), anyList(), any())).thenReturn(1L);
        lenient().when(luaScriptService.executeScript(eq("VALIDATE_ROUTE"), anyList(), anyList(), any())).thenReturn(1L);
        lenient().when(luaScriptService.executeScript(eq("CLEANUP_EXPIRED_CONNECTIONS"), anyList(), anyList(), any())).thenReturn(5L);
        
        // Ensure node name is reset
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, "test-node");
        } catch (Exception e) {
            // Ignore errors
        }
    }

    @Test
    public void testCreateConnection_WithRedisEnabled() {
        String connectionId = "redis-test-connection";
        String nodeName = "test-node:8080";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, nodeName);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Execute test
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results
        assertNotNull(emitter, "Connection should be created successfully when Redis is enabled");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established when Redis is enabled");
        
        // Verify atomic operation was called
        verify(luaScriptService, times(1)).executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any());
    }

    @Test
    public void testCloseConnection_WithRedisEnabled() {
        String connectionId = "redis-close-test-connection";
        String nodeName = "test-node:8080";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, nodeName);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Create connection first
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
        
        // Execute test - close connection
        sseService.closeConnection(connectionId);
        
        // Verify results
        assertFalse(sseService.isConnected(connectionId), "Connection should be closed");
        
        // Verify Redis operations were called
        verify(luaScriptService, atLeastOnce()).executeScript(eq("UNREGISTER_CONNECTION"), anyList(), anyList(), any());
    }

    @Test
    public void testGetClusterConnectionCount_WithRedisEnabled() {
        // Mock Redis return value
        when(setOperations.size(anyString())).thenReturn(100L);
        
        // Execute test
        int clusterCount = sseService.getClusterConnectionCount();
        
        // Verify results
        assertEquals(100, clusterCount, "Cluster connection count should be returned correctly");
        verify(setOperations, times(1)).size(anyString());
    }

    @Test
    public void testGetClusterConnectionCount_WithRedisDisabled() {
        // Disable Redis
        redisConfig.setEnabled(false);
        
        // Execute test
        int clusterCount = sseService.getClusterConnectionCount();
        
        // Verify results
        assertEquals(0, clusterCount, "Cluster connection count should be 0 when Redis is disabled");
        verify(setOperations, never()).size(anyString());
    }

    @Test
    public void testGetRoute_WithExistingRoute() {
        String connectionId = "route-test-connection";
        String expectedNode = "test-node:8080";
        String routeKey = redisConfig.getPrefix() + "routes:" + connectionId;
        
        // Mock Redis return value
        when(valueOperations.get(routeKey)).thenReturn(expectedNode);
        
        // Call private method via reflection
        try {
            java.lang.reflect.Method getRouteMethod = SseServiceImpl.class.getDeclaredMethod(
                "getRoute", String.class);
            getRouteMethod.setAccessible(true);
            
            String actualNode = (String) getRouteMethod.invoke(sseService, connectionId);
            
            // Verify results
            assertEquals(expectedNode, actualNode, "Route information should be returned correctly");
            verify(valueOperations, times(1)).get(routeKey);
        } catch (Exception e) {
            fail("Exception occurred while calling getRoute method: " + e.getMessage());
        }
    }

    @Test
    public void testGetRoute_WithNonExistingRoute() {
        String connectionId = "non-existing-route-test-connection";
        String routeKey = redisConfig.getPrefix() + "routes:" + connectionId;
        
        // Mock Redis return null
        when(valueOperations.get(routeKey)).thenReturn(null);
        
        // Call private method via reflection
        try {
            java.lang.reflect.Method getRouteMethod = SseServiceImpl.class.getDeclaredMethod(
                "getRoute", String.class);
            getRouteMethod.setAccessible(true);
            
            String actualNode = (String) getRouteMethod.invoke(sseService, connectionId);
            
            // Verify results
            assertNull(actualNode, "Non-existing route information should return null");
            verify(valueOperations, times(1)).get(routeKey);
        } catch (Exception e) {
            fail("Exception occurred while calling getRoute method: " + e.getMessage());
        }
    }

    @Test
    public void testSendMessageToConnection_LocalConnection() {
        String connectionId = "local-connection";
        String eventName = "test-event";
        String message = "test message";
        
        // Create local connection first
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Local connection should be created successfully");
        
        // Execute test
        assertDoesNotThrow(() -> sseService.sendMessageToConnection(connectionId, eventName, message),
                "Sending message to local connection should not throw exception");
    }

    @Test
    public void testSendMessageToConnection_RemoteConnection() {
        String connectionId = "remote-connection";
        String eventName = "test-event";
        String message = "test message";
        String targetNode = "remote-node:8080";
        String localNode = "local-node:8080";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, localNode);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Mock route information
        String routeKey = redisConfig.getPrefix() + "routes:" + connectionId;
        when(valueOperations.get(routeKey)).thenReturn(targetNode);
        
        // Execute test
        sseService.sendMessageToConnection(connectionId, eventName, message);
        
        // Verify command was sent to remote node
        String commandChannel = redisConfig.getPrefix() + "commands:" + targetNode;
        verify(redisTemplate, times(1)).convertAndSend(eq(commandChannel), anyString());
    }

    @Test
    public void testBroadcastToCluster_WithRedisEnabled() {
        String eventName = "cluster-event";
        String message = "cluster message";
        
        // Execute test
        sseService.broadcastToCluster(eventName, message);
        
        // Verify local broadcast
        assertEquals(0, sseService.getConnectionCount(), "Local connection count should be 0");
        
        // Verify Redis broadcast
        verify(redisTemplate, times(1)).convertAndSend(eq(redisConfig.getChannel()), anyString());
    }

    @Test
    public void testCheckConnectionConsistency_WithOrphanConnections() {
        String nodeName = "test-node:8080";
        String connectionId = "orphan-connection";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, nodeName);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Mock Redis return orphan connections
        String nodeSetKey = redisConfig.getPrefix() + "nodes:" + nodeName + ":connections";
        Set<Object> redisConnections = new HashSet<>();
        redisConnections.add(connectionId);
        when(setOperations.members(nodeSetKey)).thenReturn(redisConnections);
        
        // Local connection does not exist, so it is an orphan connection
        
        // Execute test - call private method via reflection
        try {
            java.lang.reflect.Method checkConsistencyMethod = SseServiceImpl.class.getDeclaredMethod(
                "checkConnectionConsistency");
            checkConsistencyMethod.setAccessible(true);
            
            checkConsistencyMethod.invoke(sseService);
            
            // Verify orphan connections are handled
            verify(luaScriptService, atLeastOnce()).executeScript(eq("UNREGISTER_CONNECTION"), anyList(), anyList(), any());
        } catch (Exception e) {
            fail("Exception occurred while calling checkConnectionConsistency method: " + e.getMessage());
        }
    }

    @Test
    public void testValidateConnectionRoutes() {
        String connectionId = "route-validation-connection";
        String nodeName = "test-node:8080";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, nodeName);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Create local connection
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        
        // Mock Lua script return value
        when(luaScriptService.executeScript(eq("VALIDATE_ROUTE"), anyList(), anyList(), any()))
            .thenReturn(1L); // Indicates route was corrected
        
        // Execute test - call private method via reflection
        try {
            java.lang.reflect.Method validateRoutesMethod = SseServiceImpl.class.getDeclaredMethod(
                "validateConnectionRoutes");
            validateRoutesMethod.setAccessible(true);
            
            validateRoutesMethod.invoke(sseService);
            
            // Verify route validation was called
            verify(luaScriptService, times(1)).executeScript(eq("VALIDATE_ROUTE"), anyList(), anyList(), any());
        } catch (Exception e) {
            fail("Exception occurred while calling validateConnectionRoutes method: " + e.getMessage());
        }
    }

    @Test
    public void testCleanupExpiredConnections_WithRedisAtomic() {
        String nodeName = "test-node:8080";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, nodeName);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Mock Lua script return value
        when(luaScriptService.executeScript(eq("CLEANUP_EXPIRED_CONNECTIONS"), anyList(), anyList(), any()))
            .thenReturn(5L); // Indicates 5 connections were cleaned up
        
        // Execute test - call private method via reflection
        try {
            java.lang.reflect.Method cleanupMethod = SseServiceImpl.class.getDeclaredMethod(
                "cleanupRedisConnectionsWithRetry");
            cleanupMethod.setAccessible(true);
            
            cleanupMethod.invoke(sseService);
            
            // Verify atomic cleanup was called
            verify(luaScriptService, times(1)).executeScript(eq("CLEANUP_EXPIRED_CONNECTIONS"), anyList(), anyList(), any());
        } catch (Exception e) {
            fail("Exception occurred while calling cleanupRedisConnectionsWithRetry method: " + e.getMessage());
        }
    }

    @Test
    public void testDetectResourceLeaks() {
        // Create a timed-out connection
        String connectionId = "leak-test-connection";
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        
        // Modify connection time to long ago (simulate timeout)
        try {
            java.lang.reflect.Field connectionTimesField = SseServiceImpl.class.getDeclaredField("connectionTimes");
            connectionTimesField.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.Map<String, Long> connectionTimes = (java.util.Map<String, Long>) connectionTimesField.get(sseService);
            connectionTimes.put(connectionId, System.currentTimeMillis() - cleanupConfig.getConnectionTtl() - 1000);
        } catch (Exception e) {
            fail("Exception occurred while modifying connection time: " + e.getMessage());
        }
        
        // Execute test - call private method via reflection
        try {
            java.lang.reflect.Method detectLeaksMethod = SseServiceImpl.class.getDeclaredMethod(
                "detectResourceLeaks");
            detectLeaksMethod.setAccessible(true);
            
            detectLeaksMethod.invoke(sseService);
            
            // Verify timed-out connection is cleaned up
            assertFalse(sseService.isConnected(connectionId), "Timed-out connection should be cleaned up");
        } catch (Exception e) {
            fail("Exception occurred while calling detectResourceLeaks method: " + e.getMessage());
        }
    }
}