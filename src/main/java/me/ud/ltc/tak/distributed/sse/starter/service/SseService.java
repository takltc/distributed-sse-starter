package me.ud.ltc.tak.distributed.sse.starter.service;

import java.util.Map;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Server-Sent Events service interface
 * 
 * @author takltc
 */
public interface SseService {

    /**
     * Create SSE connection
     * 
     * @param connectionId Connection ID
     * @return SseEmitter object
     */
    SseEmitter createConnection(String connectionId);

    /**
     * Send message to specified connection
     * 
     * @param connectionId Connection ID
     * @param eventName Event name
     * @param data Data
     */
    void sendMessage(String connectionId, String eventName, Object data);

    /**
     * Broadcast message to all connections
     * 
     * @param eventName Event name
     * @param data Data
     */
    void broadcast(String eventName, Object data);

    /**
     * Send message to specified connection (supports cross-node)
     * 
     * @param connectionId Connection ID
     * @param eventName Event name
     * @param data Data
     */
    void sendMessageToConnection(String connectionId, String eventName, Object data);

    /**
     * Broadcast message to cluster
     * 
     * @param eventName Event name
     * @param data Data
     */
    void broadcastToCluster(String eventName, Object data);

    /**
     * Close specified connection
     * 
     * @param connectionId Connection ID
     */
    void closeConnection(String connectionId);

    /**
     * Close all connections
     */
    void closeAllConnections();

    /**
     * Check if connection is active
     * 
     * @param connectionId Connection ID
     * @return Whether the connection is active
     */
    boolean isConnected(String connectionId);

    /**
     * Get local connection count
     * 
     * @return Local connection count
     */
    int getConnectionCount();

    /**
     * Get connection time mapping
     * 
     * @return Connection time mapping
     */
    Map<String, Long> getConnectionTimes();

    /**
     * Get cluster connection count
     * 
     * @return Cluster connection count
     */
    int getClusterConnectionCount();

    /**
     * Get node name
     * 
     * @return Node name
     */
    String getNodeName();
}