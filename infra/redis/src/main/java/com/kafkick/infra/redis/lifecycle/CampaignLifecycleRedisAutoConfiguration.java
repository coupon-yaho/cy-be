package com.kafkick.infra.redis.lifecycle;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import com.kafkick.core.observation.CampaignLifecycleRecorder;

import tools.jackson.databind.ObjectMapper;

@AutoConfiguration(
        after = DataRedisAutoConfiguration.class,
        afterName = "com.kafkick.api.observation.ApiObservationAutoConfiguration"
)
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

    @Bean(name = "campaignLifecycleRedisMessageListenerContainer")
    @ConditionalOnProperty(
            prefix = "campaign.lifecycle.redis",
            name = "subscriber-enabled",
            havingValue = "true"
    )
    @ConditionalOnBean({
            RedisConnectionFactory.class,
            CampaignLifecycleRecorder.class
    })
    @ConditionalOnMissingBean(name =
            "campaignLifecycleRedisMessageListenerContainer")
    RedisMessageListenerContainer campaignLifecycleRedisMessageListenerContainer(
            RedisConnectionFactory connectionFactory,
            ObjectMapper objectMapper,
            CampaignLifecycleRecorder recorder,
            CampaignLifecycleRedisProperties properties
    ) {
        RedisCampaignClosedEventSubscriber subscriber =
                new RedisCampaignClosedEventSubscriber(
                        objectMapper,
                        recorder
                );
        RedisMessageListenerContainer container =
                new RecoveringRedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(
                subscriber,
                new ChannelTopic(properties.getChannel())
        );
        return container;
    }
}
