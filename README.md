# Distributed SSE Starter

A Spring Boot starter library for distributed Server-Sent Events (SSE) implementation. This library provides a robust, scalable solution for managing SSE connections across multiple nodes in a cluster using Redis for coordination.

## Overview

Server-Sent Events (SSE) enable servers to push real-time updates to clients over a single HTTP connection. While simple to implement in single-node applications, scaling SSE across multiple nodes in a cluster introduces challenges like connection management, message routing, and state consistency.

This library addresses these challenges by providing:

- **Local Connection Management**: Efficient connection handling using ConcurrentHashMap
- **Distributed Coordination**: Redis-based clustering for multi-node deployments
- **Health Monitoring**: Heartbeat mechanism to maintain connection health
- **Resource Management**: Automatic cleanup of expired connections
- **Cross-Node Communication**: Seamless message routing and broadcasting
- **Reliability Features**: Retry mechanisms, consistency checks, and graceful shutdown

## Key Features

### Connection Management
- Local connection storage using thread-safe ConcurrentHashMap
- Configurable connection limits and timeouts
- Heartbeat mechanism to detect and handle stale connections

### Distributed Coordination
- Redis-based cluster coordination for multi-node deployments
- Automatic connection registration/unregistration with TTL
- Route mapping for efficient cross-node communication
- Cluster-wide connection tracking and monitoring

### Message Handling
- Direct message sending to specific connections
- Broadcast messaging to all connected clients
- Cross-node message routing and broadcasting via Redis Pub/Sub
- Control channels for node-to-node commands

### Reliability & Resilience
- Atomic operations using Redis Lua scripts
- Retry mechanisms with configurable backoff strategies
- Resource leak detection and automatic cleanup
- Consistency checks between local and Redis state
- Graceful shutdown procedures with connection cleanup

### Configuration Options
- Extensive configuration properties for tuning behavior
- Flexible timeout and TTL settings
- Customizable retry policies
- Toggleable features for different deployment scenarios

## Architecture

### Core Components

1. **SseService**: Main interface for SSE operations
2. **SseServiceImpl**: Implementation with local connection management and Redis coordination
3. **SseAutoConfiguration**: Auto-configuration for Spring Boot integration
4. **SseProperties**: Configuration properties with extensive customization options
5. **LuaScriptService**: Handles Redis Lua scripts for atomic operations

### Redis Integration

- Connection registration/unregistration with TTL
- Route mapping for cross-node communication
- Cluster-wide connection tracking
- Pub/Sub for broadcasting messages
- Control channels for node-to-node commands

## Installation

Add the dependency to your Maven project:

```xml
<dependency>
    <groupId>me.ud.ltc.tak</groupId>
    <artifactId>distributed-sse-starter</artifactId>
    <version>0.0.1</version>
</dependency>
```

Or for Gradle:

```gradle
implementation 'me.ud.ltc.tak:distributed-sse-starter:0.0.1'
```

## Configuration

All configuration properties are prefixed with `takltc.sse`:

```yaml
takltc:
  sse:
    # Connection configuration
    connection:
      # SSE connection timeout (milliseconds)
      timeout: 30000
      # Heartbeat interval (milliseconds)
      heartbeat: 30000
      # Maximum number of connections
      max-connections: 1000
    
    # Redis configuration (for cluster support)
    redis:
      # Whether to enable Redis support
      enabled: true
      # Redis key prefix
      prefix: "sse:"
      # Connection TTL (seconds)
      connection-ttl: 3600
      # Broadcast channel name
      channel: "sse:events"
    
    # Lua script configuration
    script:
      # Lua script retry count
      retry-count: 3
      # Lua script retry interval (milliseconds)
      retry-interval: 100
      # Whether to enable script caching
      script-cache-enabled: true
    
    # Cleanup configuration
    cleanup:
      # Whether to enable cleanup functionality
      enabled: true
      # Cleanup interval (milliseconds)
      interval: 60000
      # Connection TTL (milliseconds)
      connection-ttl: 3600000
    
    # Atomic operation configuration
    atomic-operation:
      # Whether to enable Redis atomic operations
      redis-atomic-enabled: true
      # Lua script retry count
      lua-script-retry-count: 3
      # Lua script retry interval (milliseconds)
      lua-script-retry-interval: 100
      # Whether to enable atomicity assurance for connection registration
      connection-registration-atomic: true
      # Whether to enable atomicity assurance for connection unregistration
      connection-unregistration-atomic: true
      # Whether to enable atomicity assurance for expired connection cleanup
      expired-connection-cleanup-atomic: true
    
    # Connection state consistency assurance configuration
    consistency:
      # Whether to enable connection state consistency assurance
      enabled: true
      # Connection state check interval (milliseconds)
      status-check-interval: 30000
      # Handling strategy when inconsistent (fail_fast|auto_correct)
      inconsistency-handling-strategy: auto_correct
      # Whether to enable inter-node connection state synchronization
      inter-node-sync-enabled: true
      # Connection route information validation interval (milliseconds)
      route-validation-interval: 60000
    
    # Resource recycling reliability configuration
    reliability:
      # Whether to enable resource recycling reliability assurance
      enabled: true
      # Resource recycling failure retry count
      cleanup-retry-count: 3
      # Resource recycling failure retry interval (milliseconds)
      cleanup-retry-interval: 1000
      # Whether to enable graceful shutdown
      graceful-shutdown: true
      # Graceful shutdown timeout (milliseconds)
      graceful-shutdown-timeout: 30000
      # Whether to enable resource leak detection
      resource-leak-detection: true
      # Resource leak detection interval (milliseconds)
      leak-detection-interval: 300000
```

## Usage

### Basic Setup

After adding the dependency, the SSE service is automatically configured. You can inject the `SseService` into your components:

```java
@RestController
@RequestMapping("/sse")
public class SseController {
    
    @Autowired
    private SseService sseService;
    
    @GetMapping("/connect/{clientId}")
    public SseEmitter connect(@PathVariable String clientId) {
        return sseService.createConnection(clientId);
    }
    
    @PostMapping("/send/{clientId}")
    public void sendMessage(@PathVariable String clientId, @RequestBody String message) {
        sseService.sendMessage(clientId, "message", message);
    }
    
    @PostMapping("/broadcast")
    public void broadcast(@RequestBody String message) {
        sseService.broadcast("message", message);
    }
}
```

### API Methods

- `createConnection(String connectionId)`: Create a new SSE connection
- `sendMessage(String connectionId, String eventName, Object data)`: Send message to specific connection
- `broadcast(String eventName, Object data)`: Broadcast message to all connections
- `sendMessageToConnection(String connectionId, String eventName, Object data)`: Send message with cross-node support
- `broadcastToCluster(String eventName, Object data)`: Broadcast message across the entire cluster
- `closeConnection(String connectionId)`: Close a specific connection
- `closeAllConnections()`: Close all connections
- `isConnected(String connectionId)`: Check if connection is active
- `getConnectionCount()`: Get local connection count
- `getClusterConnectionCount()`: Get cluster-wide connection count

## Development

### Building the Project

```bash
mvn clean install
```

### Running Tests

```bash
# Run all tests
mvn test

# Run integration tests
mvn test -Dtest="*IntegrationTest"

# Run performance tests
mvn test -Dtest="*PerformanceTest"

# Run specific test class
mvn test -Dtest=SseServiceImplTest
```

### Package Structure

- `me.ud.ltc.tak.distributed.sse.starter.config`: Configuration classes
- `me.ud.ltc.tak.distributed.sse.starter.script`: Lua script handling
- `me.ud.ltc.tak.distributed.sse.starter.service`: Service interfaces and implementations
- `me.ud.ltc.tak.distributed.sse.starter.vo`: Value objects for API communication

### Development Guidelines

1. **Thread Safety**: The implementation uses ConcurrentHashMap and atomic operations extensively. Be careful when modifying shared state.

2. **Redis Operations**: Critical Redis operations use Lua scripts for atomicity. Modifications to these scripts should be tested thoroughly.

3. **Error Handling**: The code includes comprehensive error handling with retry mechanisms. Follow the existing patterns when adding new operations.

4. **Resource Management**: Connections and thread pools are cleaned up during shutdown. Follow the existing patterns for resource management.

5. **Configuration**: New features should be configurable through SseProperties to maintain flexibility.

## Testing Strategy

The project includes comprehensive tests covering:

- Unit tests for individual components
- Integration tests with Redis
- Performance tests for high-load scenarios
- Boundary condition tests
- Exception handling tests
- Atomic operation tests

Tests are located in `src/test/java/me/ud/ltc/tak/distributed/sse/starter/`.

## License

This project is licensed under the MIT License.