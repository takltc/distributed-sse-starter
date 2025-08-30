package me.ud.ltc.tak.distributed.sse.starter.script;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test class for Lua script service
 * Does not depend on Spring context, directly tests core functionality
 */
public class LuaScriptServiceSimpleTest {

    @Test
    public void testGetScriptContent() {
        // Create LuaScriptService instance with default configuration
        LuaScriptServiceImpl luaScriptService = new LuaScriptServiceImpl(null);
        
        // Test getting script content functionality
        String registerScriptContent = luaScriptService.getScriptContent("REGISTER_CONNECTION");
        assertNotNull(registerScriptContent, "Register connection script content should not be null");
        assertFalse(registerScriptContent.isEmpty(), "Register connection script content should not be empty");
        assertTrue(registerScriptContent.contains("SETEX"), "Script content should contain SETEX command");

        String unregisterScriptContent = luaScriptService.getScriptContent("UNREGISTER_CONNECTION");
        assertNotNull(unregisterScriptContent, "Unregister connection script content should not be null");
        assertFalse(unregisterScriptContent.isEmpty(), "Unregister connection script content should not be empty");
        assertTrue(unregisterScriptContent.contains("DEL"), "Script content should contain DEL command");

        String validateScriptContent = luaScriptService.getScriptContent("VALIDATE_ROUTE");
        assertNotNull(validateScriptContent, "Validate route script content should not be null");
        assertFalse(validateScriptContent.isEmpty(), "Validate route script content should not be empty");

        String cleanupScriptContent = luaScriptService.getScriptContent("CLEANUP_EXPIRED_CONNECTIONS");
        assertNotNull(cleanupScriptContent, "Cleanup expired connections script content should not be null");
        assertFalse(cleanupScriptContent.isEmpty(), "Cleanup expired connections script content should not be empty");
    }

    @Test
    public void testReloadScript() {
        // Create LuaScriptService instance with caching enabled
        LuaScriptProperties properties = new LuaScriptProperties();
        properties.setScriptCacheEnabled(true);
        LuaScriptServiceImpl luaScriptService = new LuaScriptServiceImpl(null, properties);

        // First get the original script content
        String originalContent = luaScriptService.getScriptContent("REGISTER_CONNECTION");
        assertNotNull(originalContent, "Original script content should not be null");

        // Reload the script
        luaScriptService.reloadScript("REGISTER_CONNECTION");

        // Get the script content again, should be the same
        String reloadedContent = luaScriptService.getScriptContent("REGISTER_CONNECTION");
        assertNotNull(reloadedContent, "Script content after reload should not be null");
        assertEquals(originalContent, reloadedContent, "Script content after reload should be consistent with original content");
    }

    @Test
    public void testReloadAllScripts() {
        // Create LuaScriptService instance
        LuaScriptServiceImpl luaScriptService = new LuaScriptServiceImpl(null);

        // Reload all scripts
        assertDoesNotThrow(() -> luaScriptService.reloadAllScripts(), "Reloading all scripts should not throw an exception");
    }

    @Test
    public void testScriptCaching() {
        // Create LuaScriptService instance with caching enabled
        LuaScriptProperties properties = new LuaScriptProperties();
        properties.setScriptCacheEnabled(true);
        LuaScriptServiceImpl luaScriptService = new LuaScriptServiceImpl(null, properties);

        // Continuously get the same script content multiple times
        String scriptName = "REGISTER_CONNECTION";
        String content1 = luaScriptService.getScriptContent(scriptName);
        String content2 = luaScriptService.getScriptContent(scriptName);
        String content3 = luaScriptService.getScriptContent(scriptName);

        // Verify content consistency
        assertEquals(content1, content2, "Caching mechanism should return the same content");
        assertEquals(content2, content3, "Caching mechanism should return the same content");
        assertNotNull(content1, "Script content should not be null");
    }

    @Test
    public void testScriptCachingDisabled() {
        // Create LuaScriptService instance with caching disabled
        LuaScriptProperties properties = new LuaScriptProperties();
        properties.setScriptCacheEnabled(false);
        LuaScriptServiceImpl luaScriptService = new LuaScriptServiceImpl(null, properties);

        // Continuously get the same script content multiple times
        String scriptName = "REGISTER_CONNECTION";
        String content1 = luaScriptService.getScriptContent(scriptName);
        String content2 = luaScriptService.getScriptContent(scriptName);

        // Verify content consistency but both are valid
        assertEquals(content1, content2, "Even with caching disabled, content should be consistent");
        assertNotNull(content1, "Script content should not be null");
    }
}