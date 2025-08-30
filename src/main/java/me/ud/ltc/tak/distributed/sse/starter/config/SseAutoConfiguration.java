package me.ud.ltc.tak.distributed.sse.starter.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import me.ud.ltc.tak.distributed.sse.starter.script.LuaScriptService;
import me.ud.ltc.tak.distributed.sse.starter.script.LuaScriptServiceImpl;
import me.ud.ltc.tak.distributed.sse.starter.service.SseService;
import me.ud.ltc.tak.distributed.sse.starter.service.impl.SseServiceImpl;

import lombok.extern.slf4j.Slf4j;

/**
 * @author takltc
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(SseProperties.class)
@ConditionalOnProperty(prefix = "takltc.sse", name = "enabled", havingValue = "true", matchIfMissing = true)
public class SseAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public LuaScriptService luaScriptService(RedisTemplate<String, Object> redisTemplate) {
        return new LuaScriptServiceImpl(redisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public SseService sseService(SseProperties sseProperties, RedisTemplate<String, Object> redisTemplate,
        RedisMessageListenerContainer redisMessageListenerContainer, LuaScriptService luaScriptService,
        String nodeNameScc) {
        log.info("Creating distributed SSE service");
        return new SseServiceImpl(sseProperties, redisTemplate, redisMessageListenerContainer, luaScriptService,
            nodeNameScc);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(RedisConnectionFactory.class)
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory connectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        return container;
    }
}