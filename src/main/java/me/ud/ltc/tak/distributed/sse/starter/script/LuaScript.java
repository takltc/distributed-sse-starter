package me.ud.ltc.tak.distributed.sse.starter.script;

import lombok.Getter;

/**
 * Lua script enumeration
 * 
 * @author takltc
 */
@Getter
public enum LuaScript {

    /**
     * Register connection script
     */
    REGISTER_CONNECTION("register_connection.lua"),

    /**
     * Unregister connection script
     */
    UNREGISTER_CONNECTION("unregister_connection.lua"),

    /**
     * Cleanup expired connections script
     */
    CLEANUP_EXPIRED_CONNECTIONS("cleanup_expired_connections.lua"),

    /**
     * Validate route script
     */
    VALIDATE_ROUTE("validate_route.lua");

    private final String fileName;

    LuaScript(String fileName) {
        this.fileName = fileName;
    }

}