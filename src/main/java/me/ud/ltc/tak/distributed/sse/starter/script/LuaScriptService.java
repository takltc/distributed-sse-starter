package me.ud.ltc.tak.distributed.sse.starter.script;

import java.util.List;

import org.springframework.data.redis.connection.ReturnType;

/**
 * Lua script service interface
 * 
 * @author takltc
 */
public interface LuaScriptService {

    /**
     * Execute Lua script
     * 
     * @param scriptName Script name
     * @param keys Key list
     * @param args Argument list
     * @param returnType Return type
     * @return Execution result
     */
    <T> T executeScript(String scriptName, List<String> keys, List<String> args, ReturnType returnType);

    /**
     * Get script content
     * 
     * @param scriptName Script name
     * @return Script content
     */
    String getScriptContent(String scriptName);

    /**
     * Reload script
     * 
     * @param scriptName Script name
     */
    void reloadScript(String scriptName);

    /**
     * Reload all scripts
     */
    void reloadAllScripts();
}