package com.kafkick.infra.redis.lifecycle;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

import tools.jackson.databind.ObjectMapper;

@AutoConfiguration(after = DataRedisAutoConfiguration.class)
@EnableConfigurationProperties(CampaignLifecycleRedisProperties.class)
public class CampaignLifecycleRedisAutoConfiguration {

    @Bean
    @ConditionalOnBean(StringRedisTemplate.class)
    @ConditionalOnMissingBean(RedisCampaignClosedEventPublisher.class)
    RedisCampaignClosedEventPublisher redisCampaignClosedEventPublisher(
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            CampaignLifecycleRedisProperties properties
    ) {
        return new RedisCampaignClosedEventPublisher(
                redisTemplate,
                objectMapper,
                properties.getChannel()
        );
    }
}
