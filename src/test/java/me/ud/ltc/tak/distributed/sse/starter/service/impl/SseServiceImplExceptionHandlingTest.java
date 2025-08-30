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
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * SSE Service Implementation Exception Handling Test
 * 
 * @author takltc
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SseServiceImplExceptionHandlingTest {

    @Mock
    private SseProperties sseProperties;

    @Mock
    private RedisTemplate<String, Object> redisTemplate;
    
    @Mock
    private ValueOperations<String, Object> valueOperations;
    
    @Mock
    private SetOperations<String, Object> setOperations;

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
        
        redisConfig.setEnabled(true);
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
        
        // Mock Lua script service return values - avoid executeWithRetry retries
        lenient().when(luaScriptService.executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any())).thenReturn(1L);
        lenient().when(luaScriptService.executeScript(eq("UNREGISTER_CONNECTION"), anyList(), anyList(), any())).thenReturn(1L);
        
        // Mock Redis operations - use lenient() to avoid unnecessary stubbing exceptions
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(redisTemplate.opsForSet()).thenReturn(setOperations);
        
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
    public void testCreateConnection_WhenRedisOperationFails() {
        String connectionId = "redis-failure-test";
        String nodeName = "test-node:8080";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, nodeName);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Mock Lua script execution failure
        when(luaScriptService.executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any()))
            .thenThrow(new RuntimeException("Redis operation failed"));
        
        // Execute test
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results - connection should still be created even if Redis operation fails
        assertNotNull(emitter, "Connection should be created even if Redis operation fails");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
        
        // Verify fallback method is called
        verify(redisTemplate.opsForValue(), atLeastOnce()).set(anyString(), any(), anyLong(), any());
    }

    @Test
    public void testCloseConnection_WhenRedisOperationFails() {
        String connectionId = "redis-close-failure-test";
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
        
        // Mock Lua script execution failure
        when(luaScriptService.executeScript(eq("UNREGISTER_CONNECTION"), anyList(), anyList(), any()))
            .thenThrow(new RuntimeException("Redis operation failed"));
        
        // Execute test
        sseService.closeConnection(connectionId);
        
        // Verify results - connection should be closed
        assertFalse(sseService.isConnected(connectionId), "Connection should be closed");
        
        // Verify fallback method is called
        verify(redisTemplate, atLeastOnce()).delete(anyString());
    }

    @Test
    public void testSendMessage_WhenEmitterThrowsException() {
        String connectionId = "emitter-failure-test";
        
        // Create connection
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        
        // Since we cannot directly mock SseEmitter's send method, we test exception handling by verifying the connection is removed
        
        // Execute test
        assertDoesNotThrow(() -> sseService.sendMessage(connectionId, "test-event", "test message"),
                "Exceptions during message sending should be caught and handled");
        
        // Note: Since we cannot simulate SseEmitter send failure, the connection will not be removed
        // In a real environment, when SseEmitter send fails, it triggers the onError callback, which then removes the connection
    }

    @Test
    public void testBroadcast_WhenSomeEmittersFail() {
        String connectionId1 = "broadcast-failure-test-1";
        String connectionId2 = "broadcast-failure-test-2";
        
        // Create connections
        sseService.createConnection(connectionId1);
        sseService.createConnection(connectionId2);
        
        assertEquals(2, sseService.getConnectionCount(), "2 connections should be created");
        
        // Execute test
        assertDoesNotThrow(() -> sseService.broadcast("test-event", "test message"),
                "Partial failures during message broadcasting should be handled");
        
        // Note: Since we cannot simulate SseEmitter send failure, the connections will not be removed
        // In a real environment, failed connections would be removed
    }

    @Test
    public void testExecuteWithRetry_WhenAllRetriesFail() {
        // Test retry mechanism by calling private method through reflection
        try {
            java.lang.reflect.Method executeWithRetryMethod = SseServiceImpl.class.getDeclaredMethod(
                "executeWithRetry", 
                SseServiceImpl.RetryableOperation.class, 
                int.class, 
                int.class, 
                String.class);
            executeWithRetryMethod.setAccessible(true);
            
            // Create an operation that always fails
            SseServiceImpl.RetryableOperation failingOperation = new SseServiceImpl.RetryableOperation() {
                @Override
                public void execute() throws Exception {
                    throw new RuntimeException("Always failing operation");
                }
            };
            
            // Execute test
            boolean result = (Boolean) executeWithRetryMethod.invoke(
                sseService, 
                failingOperation, 
                3,  // 3 retries
                10, // 10ms interval
                "Test operation"
            );
            
            // Verify results
            assertFalse(result, "Should return false when all retries fail");
        } catch (Exception e) {
            fail("Exception occurred while calling executeWithRetry method: " + e.getMessage());
        }
    }

    @Test
    public void testExecuteWithRetry_WhenSuccessAfterRetry() {
        // Test retry mechanism by calling private method through reflection
        try {
            java.lang.reflect.Method executeWithRetryMethod = SseServiceImpl.class.getDeclaredMethod(
                "executeWithRetry", 
                SseServiceImpl.RetryableOperation.class, 
                int.class, 
                int.class, 
                String.class);
            executeWithRetryMethod.setAccessible(true);
            
            // Create an operation that succeeds on the second attempt
            final int[] attemptCount = {0};
            SseServiceImpl.RetryableOperation sometimesFailingOperation = new SseServiceImpl.RetryableOperation() {
                @Override
                public void execute() throws Exception {
                    attemptCount[0]++;
                    if (attemptCount[0] < 2) {
                        throw new RuntimeException("Failing on first attempt");
                    }
                    // Succeeds on second attempt
                }
            };
            
            // Execute test
            boolean result = (Boolean) executeWithRetryMethod.invoke(
                sseService, 
                sometimesFailingOperation, 
                3,  // 3 retries
                10, // 10ms interval
                "Test operation"
            );
            
            // Verify results
            assertTrue(result, "Should return true after successful retry");
            assertEquals(2, attemptCount[0], "Should succeed on second attempt");
        } catch (Exception e) {
            fail("Exception occurred while calling executeWithRetry method: " + e.getMessage());
        }
    }

    @Test
    public void testExecuteWithRetry_WhenInterruptedException() {
        // Test retry mechanism by calling private method through reflection
        try {
            java.lang.reflect.Method executeWithRetryMethod = SseServiceImpl.class.getDeclaredMethod(
                "executeWithRetry", 
                SseServiceImpl.RetryableOperation.class, 
                int.class, 
                int.class, 
                String.class);
            executeWithRetryMethod.setAccessible(true);
            
            // Create an operation that throws InterruptedException on first attempt
            SseServiceImpl.RetryableOperation interruptingOperation = new SseServiceImpl.RetryableOperation() {
                @Override
                public void execute() throws Exception {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Operation interrupted");
                }
            };
            
            // Execute test
            boolean result = (Boolean) executeWithRetryMethod.invoke(
                sseService, 
                interruptingOperation, 
                3,  // 3 retries
                10, // 10ms interval
                "Interrupt test operation"
            );
            
            // Verify results
            assertFalse(result, "Should return false when interrupted");
            assertTrue(Thread.currentThread().isInterrupted(), "Thread should be marked as interrupted");
            
            // Clear interrupt status
            Thread.interrupted();
        } catch (Exception e) {
            fail("Exception occurred while calling executeWithRetry method: " + e.getMessage());
        }
    }

    @Test
    public void testCleanupExpiredConnections_WhenRedisScanFails() {
        // Enable Redis atomic operations
        atomicOperationConfig.setRedisAtomicEnabled(true);
        atomicOperationConfig.setExpiredConnectionCleanupAtomic(true);
        
        // Mock Lua script execution failure
        when(luaScriptService.executeScript(eq("CLEANUP_EXPIRED_CONNECTIONS"), anyList(), anyList(), any()))
            .thenThrow(new RuntimeException("Redis scan failed"));
        
        // Call private method through reflection
        try {
            java.lang.reflect.Method cleanupMethod = SseServiceImpl.class.getDeclaredMethod(
                "cleanupRedisConnectionsWithRetry");
            cleanupMethod.setAccessible(true);
            
            // Execute test
            cleanupMethod.invoke(sseService);
            
            // Verify fallback method is called
            // Since we cannot directly verify private method calls, we verify Redis template is called
            verify(redisTemplate, atLeastOnce()).execute(any(org.springframework.data.redis.core.RedisCallback.class));
        } catch (Exception e) {
            fail("Exception occurred while calling cleanupRedisConnectionsWithRetry method: " + e.getMessage());
        }
    }

    @Test
    public void testHandleCommand_WithInvalidCommandFormat() {
        // Call private method through reflection
        try {
            java.lang.reflect.Method handleCommandMethod = SseServiceImpl.class.getDeclaredMethod(
                "handleCommand", String.class);
            handleCommandMethod.setAccessible(true);
            
            // Execute test - invalid command format
            assertDoesNotThrow(() -> handleCommandMethod.invoke(sseService, "INVALID_FORMAT"),
                "Handling invalid command format should not throw an exception");
            
            // Execute test - empty command
            assertDoesNotThrow(() -> handleCommandMethod.invoke(sseService, ""),
                "Handling empty command should not throw an exception");
            
            // Execute test - null command
            assertDoesNotThrow(() -> handleCommandMethod.invoke(sseService, (String) null),
                "Handling null command should not throw an exception");
        } catch (Exception e) {
            fail("Exception occurred while calling handleCommand method: " + e.getMessage());
        }
    }

    @Test
    public void testHandleCommand_WithSendMessageCommand_Malformed() {
        // Call private method through reflection
        try {
            java.lang.reflect.Method handleCommandMethod = SseServiceImpl.class.getDeclaredMethod(
                "handleCommand", String.class);
            handleCommandMethod.setAccessible(true);
            
            // Execute test - malformed SEND_MESSAGE command
            assertDoesNotThrow(() -> handleCommandMethod.invoke(sseService, "SEND_MESSAGE:insufficient:parameters"),
                "Handling malformed SEND_MESSAGE command should not throw an exception");
        } catch (Exception e) {
            fail("Exception occurred while calling handleCommand method: " + e.getMessage());
        }
    }

    @Test
    public void testSetupRedisListener_WhenMessageListenerFails() {
        // Mock Redis connection failure
        doThrow(new RuntimeException("Redis connection failed"))
            .when(redisMessageListenerContainer)
            .addMessageListener(any(org.springframework.data.redis.connection.MessageListener.class), 
                              any(org.springframework.data.redis.listener.Topic.class));
        
        // Since setupRedisListener is called in @PostConstruct, we cannot test it directly
        // But we can verify the service still works properly
        
        String connectionId = "redis-listener-failure-test";
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        
        assertNotNull(emitter, "Connection should be created even if Redis listener setup fails");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
    }

    @Test
    public void testGetRoute_WhenRedisOperationFails() {
        String connectionId = "route-failure-test";
        String routeKey = redisConfig.getPrefix() + "routes:" + connectionId;
        
        // Mock Redis operation failure
        when(valueOperations.get(routeKey))
            .thenThrow(new RuntimeException("Redis get failed"));
        
        // Call private method through reflection
        try {
            java.lang.reflect.Method getRouteMethod = SseServiceImpl.class.getDeclaredMethod(
                "getRoute", String.class);
            getRouteMethod.setAccessible(true);
            
            String result = (String) getRouteMethod.invoke(sseService, connectionId);
            
            // Verify results
            assertNull(result, "Should return null when Redis operation fails");
        } catch (Exception e) {
            fail("Exception occurred while calling getRoute method: " + e.getMessage());
        }
    }

    @Test
    public void testShutdown_WhenThreadPoolShutdownFails() {
        // Since shutdown is called in @PreDestroy, we cannot test it directly
        // But we can verify the resource cleanup logic by creating multiple connections
        
        String connectionId = "shutdown-test";
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        assertNotNull(emitter, "Connection should be created successfully");
        
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
        
        // Verify the service can shut down properly (since we cannot directly call the @PreDestroy method)
        assertDoesNotThrow(() -> sseService.closeAllConnections(),
                "Closing all connections should not throw an exception");
        
        assertFalse(sseService.isConnected(connectionId), "Connection should be closed");
    }
}