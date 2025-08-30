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
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * SSE Service Implementation Atomic Operation Test
 * 
 * @author takltc
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SseServiceImplAtomicOperationTest {

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
    }

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

        // Configure atomic operation related values
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
        atomicOperationConfig.setRedisOperationTimeout(3000);
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
        reliabilityConfig.setCleanupTimeout(10000);
        reliabilityConfig.setGracefulShutdown(true);
        reliabilityConfig.setGracefulShutdownTimeout(30000);
        reliabilityConfig.setResourceLeakDetection(true);
        reliabilityConfig.setLeakDetectionInterval(300000);

        // Mock configuration properties
        when(sseProperties.getConnection()).thenReturn(connectionConfig);
        when(sseProperties.getRedis()).thenReturn(redisConfig);
        when(sseProperties.getCleanup()).thenReturn(cleanupConfig);
        when(sseProperties.getAtomicOperation()).thenReturn(atomicOperationConfig);
        when(sseProperties.getConsistency()).thenReturn(consistencyConfig);
        when(sseProperties.getReliability()).thenReturn(reliabilityConfig);

        // Mock Redis operations
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        
        // Ensure node name cache is reset
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, null);
        } catch (Exception e) {
            // Ignore errors
        }
    }

    @Test
    public void testRegisterConnection_AtomicSuccess() {
        String connectionId = "atomic-register-success";
        String nodeName = "test-node:8080";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, nodeName);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Mock successful Lua script execution
        when(luaScriptService.executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any()))
            .thenReturn(1L);
        
        // Execute test
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results
        assertNotNull(emitter, "Connection should be created successfully");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
        
        // Verify atomic operation was called
        verify(luaScriptService, times(1)).executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any());
    }

    @Test
    public void testRegisterConnection_AtomicFailure_Fallback() {
        String connectionId = "atomic-register-fallback";
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
            .thenThrow(new RuntimeException("Lua script execution failed"));
        
        // Execute test
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results
        assertNotNull(emitter, "Connection should be created even if atomic operation fails");
        assertTrue(sseService.isConnected(connectionId), "Connection should be established");
        
        // Verify atomic operation was called (failure triggers retry, count >= 1)
        verify(luaScriptService, atLeastOnce()).executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any());
        
        // Verify fallback operation was called
        verify(redisTemplate.opsForValue(), atLeastOnce()).set(anyString(), any(), anyLong(), any());
    }

    @Test
    public void testUnregisterConnection_AtomicSuccess() {
        String connectionId = "atomic-unregister-success";
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
        
        // Mock successful Lua script execution
        when(luaScriptService.executeScript(eq("UNREGISTER_CONNECTION"), anyList(), anyList(), any()))
            .thenReturn(1L);
        
        // Execute test
        sseService.closeConnection(connectionId);
        
        // Verify results
        assertFalse(sseService.isConnected(connectionId), "Connection should be closed");
        
        // Verify atomic operation was called
        verify(luaScriptService, times(1)).executeScript(eq("UNREGISTER_CONNECTION"), anyList(), anyList(), any());
    }

    @Test
    public void testUnregisterConnection_AtomicFailure_Fallback() {
        String connectionId = "atomic-unregister-fallback";
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
            .thenThrow(new RuntimeException("Lua script execution failed"));
        
        // Execute test
        sseService.closeConnection(connectionId);
        
        // Verify results
        assertFalse(sseService.isConnected(connectionId), "Connection should be closed");
        
        // Verify atomic operation was called (failure triggers retry, count >= 1)
        verify(luaScriptService, atLeastOnce()).executeScript(eq("UNREGISTER_CONNECTION"), anyList(), anyList(), any());
        
        // Verify fallback operation was called
        verify(redisTemplate, atLeastOnce()).delete(anyString());
    }

    @Test
    public void testCleanupExpiredConnections_AtomicSuccess() {
        String nodeName = "test-node:8080";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, nodeName);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Mock successful Lua script execution
        when(luaScriptService.executeScript(eq("CLEANUP_EXPIRED_CONNECTIONS"), anyList(), anyList(), any()))
            .thenReturn(5L); // Cleaned up 5 connections
        
        // Call private method via reflection
        try {
            java.lang.reflect.Method cleanupMethod = SseServiceImpl.class.getDeclaredMethod(
                "cleanupRedisConnectionsWithRetry");
            cleanupMethod.setAccessible(true);
            
            // Execute test
            cleanupMethod.invoke(sseService);
            
            // Verify atomic operation was called
            verify(luaScriptService, times(1)).executeScript(eq("CLEANUP_EXPIRED_CONNECTIONS"), anyList(), anyList(), any());
        } catch (Exception e) {
            fail("Exception occurred while calling cleanupRedisConnectionsWithRetry method: " + e.getMessage());
        }
    }

    @Test
    public void testCleanupExpiredConnections_AtomicFailure_Fallback() {
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
        when(luaScriptService.executeScript(eq("CLEANUP_EXPIRED_CONNECTIONS"), anyList(), anyList(), any()))
            .thenThrow(new RuntimeException("Lua script execution failed"));
        
        // Call private method via reflection
        try {
            java.lang.reflect.Method cleanupMethod = SseServiceImpl.class.getDeclaredMethod(
                "cleanupRedisConnectionsWithRetry");
            cleanupMethod.setAccessible(true);
            
            // Execute test
            cleanupMethod.invoke(sseService);
            
            // Verify atomic operation was called (failure triggers retry, count >= 1)
            verify(luaScriptService, atLeastOnce()).executeScript(eq("CLEANUP_EXPIRED_CONNECTIONS"), anyList(), anyList(), any());
            
            // Verify fallback operation was called (via RedisTemplate.execute)
            verify(redisTemplate, atLeastOnce()).execute(any(org.springframework.data.redis.core.RedisCallback.class));
        } catch (Exception e) {
            fail("Exception occurred while calling cleanupRedisConnectionsWithRetry method: " + e.getMessage());
        }
    }

    @Test
    public void testValidateRoute_AtomicOperation() {
        String connectionId = "validate-route-test";
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
        
        // Mock Lua script returns route inconsistency (needs correction)
        when(luaScriptService.executeScript(eq("VALIDATE_ROUTE"), anyList(), anyList(), any()))
            .thenReturn(1L); // Indicates route was corrected
        
        // Call private method via reflection
        try {
            java.lang.reflect.Method validateRouteMethod = SseServiceImpl.class.getDeclaredMethod(
                "validateConnectionRoutes");
            validateRouteMethod.setAccessible(true);
            
            // Execute test
            validateRouteMethod.invoke(sseService);
            
            // Verify atomic operation was called
            verify(luaScriptService, times(1)).executeScript(eq("VALIDATE_ROUTE"), anyList(), anyList(), any());
        } catch (Exception e) {
            fail("Exception occurred while calling validateConnectionRoutes method: " + e.getMessage());
        }
    }

    @Test
    public void testAtomicOperation_WithZeroRetryCount() {
        // Set retry count to 0
        atomicOperationConfig.setLuaScriptRetryCount(0);
        
        String connectionId = "zero-retry-atomic";
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
            .thenThrow(new RuntimeException("Lua script execution failed"));
        
        // Execute test
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results
        assertNotNull(emitter, "Connection should be created even if atomic operation fails");
        
        // Verify atomic operation was called only once (no retries)
        verify(luaScriptService, times(1)).executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any());
    }

    @Test
    public void testAtomicOperation_WithMaximumRetryCount() {
        // Set retry count to a reasonable value
        atomicOperationConfig.setLuaScriptRetryCount(5);
        atomicOperationConfig.setLuaScriptRetryInterval(10); // Reasonable interval
        
        String connectionId = "max-retry-atomic";
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
            .thenThrow(new RuntimeException("Lua script execution failed"));
        
        // Execute test
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results
        assertNotNull(emitter, "Connection should be created even if atomic operation fails");
        
        // Verify atomic operation was called (specific count depends on implementation, but at least once)
        verify(luaScriptService, atLeastOnce()).executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any());
    }

    @Test
    public void testAtomicOperation_WithDisabledRedisAtomic() {
        // Disable Redis atomic operations
        atomicOperationConfig.setRedisAtomicEnabled(false);
        
        String connectionId = "disabled-atomic";
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
        assertNotNull(emitter, "Connection should be created successfully");
        
        // Verify Lua script was not called (because atomic operations are disabled)
        verify(luaScriptService, never()).executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any());
        
        // Verify fallback operation was called
        verify(redisTemplate.opsForValue(), atLeastOnce()).set(anyString(), any(), anyLong(), any());
    }

    @Test
    public void testConnectionRegistration_AtomicDisabled() {
        // Disable connection registration atomic guarantee
        atomicOperationConfig.setConnectionRegistrationAtomic(false);
        
        String connectionId = "registration-atomic-disabled";
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
        assertNotNull(emitter, "Connection should be created successfully");
        
        // Verify Lua script was not called (because connection registration atomic guarantee is disabled)
        verify(luaScriptService, never()).executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any());
        
        // Verify fallback operation was called
        verify(redisTemplate.opsForValue(), atLeastOnce()).set(anyString(), any(), anyLong(), any());
    }

    @Test
    public void testConnectionUnregistration_AtomicDisabled() {
        // Disable connection unregistration atomic guarantee
        atomicOperationConfig.setConnectionUnregistrationAtomic(false);
        
        String connectionId = "unregistration-atomic-disabled";
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
        
        // Execute test
        sseService.closeConnection(connectionId);
        
        // Verify results
        assertFalse(sseService.isConnected(connectionId), "Connection should be closed");
        
        // Verify Lua script was not called (because connection unregistration atomic guarantee is disabled)
        verify(luaScriptService, never()).executeScript(eq("UNREGISTER_CONNECTION"), anyList(), anyList(), any());
        
        // Verify fallback operation was called
        verify(redisTemplate, atLeastOnce()).delete(anyString());
    }

    @Test
    public void testExpiredConnectionCleanup_AtomicDisabled() {
        // Disable expired connection cleanup atomic guarantee
        atomicOperationConfig.setExpiredConnectionCleanupAtomic(false);
        
        String nodeName = "test-node:8080";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, nodeName);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Call private method via reflection
        try {
            java.lang.reflect.Method cleanupMethod = SseServiceImpl.class.getDeclaredMethod(
                "cleanupRedisConnectionsWithRetry");
            cleanupMethod.setAccessible(true);
            
            // Execute test
            cleanupMethod.invoke(sseService);
            
            // Verify Lua script was not called (because expired connection cleanup atomic guarantee is disabled)
            verify(luaScriptService, never()).executeScript(eq("CLEANUP_EXPIRED_CONNECTIONS"), anyList(), anyList(), any());
            
            // Verify fallback operation was called
            verify(redisTemplate, atLeastOnce()).execute(any(org.springframework.data.redis.core.RedisCallback.class));
        } catch (Exception e) {
            fail("Exception occurred while calling cleanupRedisConnectionsWithRetry method: " + e.getMessage());
        }
    }

    @Test
    public void testAtomicOperation_TimeoutConfiguration() {
        // Set operation timeout
        atomicOperationConfig.setRedisOperationTimeout(1);
        
        String connectionId = "timeout-atomic";
        String nodeName = "test-node:8080";
        
        // Mock getNodeName method return value
        try {
            java.lang.reflect.Field nodeNameField = SseServiceImpl.class.getDeclaredField("nodeName");
            nodeNameField.setAccessible(true);
            nodeNameField.set(sseService, nodeName);
        } catch (Exception e) {
            fail("Exception occurred while setting node name: " + e.getMessage());
        }
        
        // Mock Lua script execution timeout
        when(luaScriptService.executeScript(eq("REGISTER_CONNECTION"), anyList(), anyList(), any()))
            .thenAnswer(invocation -> {
                // Simulate timeout
                Thread.sleep(10);
                return 1L;
            });
        
        // Execute test
        org.springframework.web.servlet.mvc.method.annotation.SseEmitter emitter = sseService.createConnection(connectionId);
        
        // Verify results
        assertNotNull(emitter, "Connection should be created even if atomic operation times out");
    }
}