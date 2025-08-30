package me.ud.ltc.tak.distributed.sse.starter.script;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

/**
 * Lua script configuration properties class
 * 
 * @author takltc
 */
@Data
@Component
@ConfigurationProperties(prefix = "takltc.sse.script")
public class LuaScriptProperties {

    /**
     * Script file path prefix
     */
    private String scriptPathPrefix = "lua/";

    /**
     * Lua script retry count
     */
    private int retryCount = 3;

    /**
     * Lua script retry interval (milliseconds)
     */
    private int retryInterval = 100;

    /**
     * Lua script execution timeout (milliseconds)
     */
    private int scriptTimeout = 5000;

    /**
     * Whether to enable script caching
     */
    private boolean scriptCacheEnabled = true;
}