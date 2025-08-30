package me.ud.ltc.tak.distributed.sse.starter.config;

import java.net.InetAddress;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * Node name configuration class for generating and managing SSE service node identifiers
 * 
 * @author takltc
 */
@Slf4j
@Configuration
public class NodeNameConfiguration {

    /**
     * Get the node name for the SSE service
     * 
     * @return Node name
     */
    @Bean
    @ConditionalOnMissingBean
    public String nodeNameScc() {
        // Prioritize getting node name from system properties
        String nodeName = System.getProperty("node.name");
        if (nodeName != null && !nodeName.isEmpty()) {
            log.info("Using node name set by system property: {}", nodeName);
            return nodeName;
        }

        // If no system property is set, generate a unique identifier based on hostname and port
        try {
            String hostName = InetAddress.getLocalHost().getHostName();
            String port = System.getProperty("server.port", "8080");
            String generatedNodeName = hostName + ":" + port;
            log.info("Generated node name: {}", generatedNodeName);
            return generatedNodeName;
        } catch (Exception e) {
            log.warn("Unable to get host information, using UUID to generate node name", e);
            // If unable to get host information, use UUID to ensure uniqueness
            String uuidNodeName = "node-" + java.util.UUID.randomUUID().toString();
            log.info("Using UUID to generate node name: {}", uuidNodeName);
            return uuidNodeName;
        }
    }
}