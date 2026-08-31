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

import com.kafkick.core.observation.CouponRoundLifecycleRecorder;

import tools.jackson.databind.ObjectMapper;

@AutoConfiguration(
        after = DataRedisAutoConfiguration.class,
        afterName = "com.kafkick.api.observation.ApiObservationAutoConfiguration"
)
@EnableConfigurationProperties(CouponRoundLifecycleRedisProperties.class)
public class CouponRoundLifecycleRedisAutoConfiguration {

    @Bean
    @ConditionalOnBean(StringRedisTemplate.class)
    @ConditionalOnMissingBean(RedisCouponRoundClosedEventPublisher.class)
    RedisCouponRoundClosedEventPublisher redisCouponRoundClosedEventPublisher(
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            CouponRoundLifecycleRedisProperties properties
    ) {
        return new RedisCouponRoundClosedEventPublisher(
                redisTemplate,
                objectMapper,
                properties.getChannel()
        );
    }

    @Bean(name = "couponRoundLifecycleRedisMessageListenerContainer")
    @ConditionalOnProperty(
            prefix = "coupon-round.lifecycle.redis",
            name = "subscriber-enabled",
            havingValue = "true"
    )
    @ConditionalOnBean({
            RedisConnectionFactory.class,
            CouponRoundLifecycleRecorder.class
    })
    @ConditionalOnMissingBean(name =
            "couponRoundLifecycleRedisMessageListenerContainer")
    RedisMessageListenerContainer couponRoundLifecycleRedisMessageListenerContainer(
            RedisConnectionFactory connectionFactory,
            ObjectMapper objectMapper,
            CouponRoundLifecycleRecorder recorder,
            CouponRoundLifecycleRedisProperties properties
    ) {
        RedisCouponRoundClosedEventSubscriber subscriber =
                new RedisCouponRoundClosedEventSubscriber(
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
