package com.kafkick.batch.coupon.round;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;

import com.kafkick.infra.redis.lifecycle.RedisCampaignClosedEventPublisher;
import com.kafkick.storage.db.MySqlContainerConfig;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "spring.flyway.enabled=true")
@Import(MySqlContainerConfig.class)
class CampaignLifecycleRedisWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("Batch 운영 컨텍스트가 Redis 종료 Publisher를 자동 설정한다")
    void autoConfigureRedisCampaignClosedEventPublisher() {
        assertThat(context.getBeansOfType(
                RedisCampaignClosedEventPublisher.class
        )).hasSize(1);
        assertThat(context.containsBean(
                "campaignLifecycleRedisMessageListenerContainer"
        )).as("Batch는 종료 이벤트를 발행하지만 캠페인 종료 채널은 구독하지 않는다")
                .isFalse();
    }
}
