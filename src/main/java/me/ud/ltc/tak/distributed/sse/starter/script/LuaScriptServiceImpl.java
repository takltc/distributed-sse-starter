package me.ud.ltc.tak.distributed.sse.starter.script;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Lua script service implementation class
 * 
 * @author takltc
 */
@Slf4j
@Service
public class LuaScriptServiceImpl implements LuaScriptService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final LuaScriptProperties luaScriptProperties;

    public LuaScriptServiceImpl(RedisTemplate<String, Object> redisTemplate, LuaScriptProperties luaScriptProperties) {
        this.redisTemplate = redisTemplate;
        this.luaScriptProperties = luaScriptProperties != null ? luaScriptProperties : new LuaScriptProperties();
    }

    public LuaScriptServiceImpl(RedisTemplate<String, Object> redisTemplate) {
        this(redisTemplate, null);
    }

    /**
     * Script cache
     */
    private final Map<String, String> scriptCache = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.info("Initializing Lua script service");
        if (luaScriptProperties.isScriptCacheEnabled()) {
            preloadScripts();
        }
    }

    /**
     * Preload all scripts
     */
    private void preloadScripts() {
        for (LuaScript script : LuaScript.values()) {
            try {
                String content = loadScriptFromFile(script.getFileName());
                scriptCache.put(script.name(), content);
                log.debug("Preloading Lua script: {}", script.name());
            } catch (Exception e) {
                log.error("Failed to preload Lua script: {}", script.name(), e);
            }
        }
    }

    @Override
    public <T> T executeScript(String scriptName, List<String> keys, List<String> args, ReturnType returnType) {
        int maxRetries = luaScriptProperties.getRetryCount();
        int retryInterval = luaScriptProperties.getRetryInterval();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                // Get script content
                String scriptContent = getScriptContent(scriptName);

                // Execute script
                return redisTemplate.execute((RedisCallback<T>)connection -> {
                    byte[] script = scriptContent.getBytes();
                    // Convert keys to byte[] array
                    byte[][] keysBytes = keys.stream().map(String::getBytes).toArray(byte[][]::new);
                    // Convert args to byte[] array
                    byte[][] argsBytes = args.stream().map(String::getBytes).toArray(byte[][]::new);

                    // Merge keys and args into one array
                    byte[][] keysAndArgs = new byte[keysBytes.length + argsBytes.length][];
                    System.arraycopy(keysBytes, 0, keysAndArgs, 0, keysBytes.length);
                    System.arraycopy(argsBytes, 0, keysAndArgs, keysBytes.length, argsBytes.length);

                    return connection.eval(script, returnType, keysBytes.length, keysAndArgs);
                });
            } catch (Exception e) {
                if (attempt < maxRetries) {
                    log.warn("Failed to execute Lua script: {}, attempt: {}/{}", scriptName, attempt + 1, maxRetries + 1, e);
                    try {
                        Thread.sleep(retryInterval);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Execution of Lua script interrupted: " + scriptName, ie);
                    }
                } else {
                    log.error("Failed to execute Lua script ultimately: {}", scriptName, e);
                    throw new RuntimeException("Failed to execute Lua script ultimately: " + scriptName, e);
                }
            }
        }

        throw new RuntimeException("Failed to execute Lua script: " + scriptName);
    }

    @Override
    public String getScriptContent(String scriptName) {
        // Check cache
        if (luaScriptProperties.isScriptCacheEnabled() && scriptCache.containsKey(scriptName)) {
            return scriptCache.get(scriptName);
        }

        try {
            // Find corresponding script filename from enum
            LuaScript scriptEnum = LuaScript.valueOf(scriptName);
            String content = loadScriptFromFile(scriptEnum.getFileName());

            // Cache script content
            if (luaScriptProperties.isScriptCacheEnabled()) {
                scriptCache.put(scriptName, content);
            }

            return content;
        } catch (Exception e) {
            log.error("Failed to get Lua script content: {}", scriptName, e);
            throw new RuntimeException("Failed to get Lua script content: " + scriptName, e);
        }
    }

    @Override
    public void reloadScript(String scriptName) {
        try {
            LuaScript scriptEnum = LuaScript.valueOf(scriptName);
            String content = loadScriptFromFile(scriptEnum.getFileName());

            // Update cache
            if (luaScriptProperties.isScriptCacheEnabled()) {
                scriptCache.put(scriptName, content);
            }

            log.info("Successfully reloaded Lua script: {}", scriptName);
        } catch (Exception e) {
            log.error("Failed to reload Lua script: {}", scriptName, e);
            throw new RuntimeException("Failed to reload Lua script: " + scriptName, e);
        }
    }

    @Override
    public void reloadAllScripts() {
        for (LuaScript script : LuaScript.values()) {
            try {
                reloadScript(script.name());
            } catch (Exception e) {
                log.error("Failed to reload Lua script: {}", script.name(), e);
            }
        }
    }

    /**
     * Load script content from file
     * 
     * @param fileName File name
     * @return Script content
     * @throws IOException IO exception
     */
    private String loadScriptFromFile(String fileName) throws IOException {
        String fullPath = luaScriptProperties.getScriptPathPrefix() + fileName;
        ClassPathResource resource = new ClassPathResource(fullPath);

        try (InputStream inputStream = resource.getInputStream()) {
            return StreamUtils.copyToString(inputStream, java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}