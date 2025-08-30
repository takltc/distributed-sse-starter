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
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * SSE Service Implementation Performance Test
 * 
 * @author takltc
 */
@ExtendWith(MockitoExtension.class)
public class SseServiceImplPerformanceTest {

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
        connectionConfig.setMaxConnections(10000); // Increase maximum connections for performance testing
        
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
    }

    @Test
    public void testCreateMultipleConnections_Performance() {
        // Record start time
        long startTime = System.currentTimeMillis();
        
        // Create large number of connections
        int connectionCount = 1000;
        for (int i = 0; i < connectionCount; i++) {
            sseService.createConnection("connection-" + i);
        }
        
        // Record end time
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify results
        assertEquals(connectionCount, sseService.getConnectionCount(), 
                "Should create the specified number of connections");
        
        // Performance check (should complete within 2 seconds)
        assertTrue(duration < 2000, "Creating 1000 connections should complete within 2 seconds, actual time: " + duration + "ms");
    }

    @Test
    public void testBroadcastToMultipleConnections_Performance() {
        // Create multiple connections
        int connectionCount = 500;
        for (int i = 0; i < connectionCount; i++) {
            sseService.createConnection("connection-" + i);
        }
        
        // Record start time
        long startTime = System.currentTimeMillis();
        
        // Perform broadcast
        sseService.broadcast("test-event", "test message");
        
        // Record end time
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify results
        assertEquals(connectionCount, sseService.getConnectionCount(), 
                "Connection count should remain unchanged");
        
        // Performance check (should complete within 1 second)
        assertTrue(duration < 1000, "Broadcasting to 500 connections should complete within 1 second, actual time: " + duration + "ms");
    }

    @Test
    public void testCloseAllConnections_Performance() {
        // Create multiple connections
        int connectionCount = 1000;
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
        
        // Performance check (should complete within 1 second)
        assertTrue(duration < 1000, "Closing 1000 connections should complete within 1 second, actual time: " + duration + "ms");
    }

    @Test
    public void testConcurrentConnectionCreation_Performance() throws InterruptedException {
        int threadCount = 10;
        int connectionsPerThread = 100;
        int totalConnections = threadCount * connectionsPerThread;
        
        Thread[] threads = new Thread[threadCount];
        
        // Record start time
        long startTime = System.currentTimeMillis();
        
        // Create concurrent threads
        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < connectionsPerThread; j++) {
                    sseService.createConnection("thread-" + threadIndex + "-conn-" + j);
                }
            });
            threads[i].start();
        }
        
        // Wait for all threads to complete
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        
        // Record end time
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify results
        assertEquals(totalConnections, sseService.getConnectionCount(), 
                "Should create the specified number of connections");
        
        // Performance check (should complete within 3 seconds)
        assertTrue(duration < 3000, "Concurrent creation of 1000 connections should complete within 3 seconds, actual time: " + duration + "ms");
    }

    @Test
    public void testHighFrequencyMessageSending_Performance() {
        // Create connection
        String connectionId = "high-frequency-test";
        sseService.createConnection(connectionId);
        
        // Record start time
        long startTime = System.currentTimeMillis();
        
        // High frequency message sending
        int messageCount = 1000;
        for (int i = 0; i < messageCount; i++) {
            sseService.sendMessage(connectionId, "event-" + i, "message-" + i);
        }
        
        // Record end time
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify results
        assertTrue(sseService.isConnected(connectionId), "Connection should still exist");
        
        // Performance check (should complete within 1 second)
        assertTrue(duration < 1000, "Sending 1000 messages should complete within 1 second, actual time: " + duration + "ms");
    }
}