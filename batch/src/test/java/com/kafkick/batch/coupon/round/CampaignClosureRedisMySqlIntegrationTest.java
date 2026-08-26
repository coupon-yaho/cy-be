package com.kafkick.batch.coupon.round;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestConstructor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.coupon.service.CouponRoundLifecycleService;
import com.kafkick.infra.redis.lifecycle.CampaignLifecycleRedisProperties;
import com.kafkick.infra.redis.lifecycle.RedisCampaignClosedEventSubscriber;
import com.kafkick.storage.db.MySqlContainerConfig;

import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "spring.flyway.enabled=true",
        "batch.scheduling.enabled=false"
})
@Import(MySqlContainerConfig.class)
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
class CampaignClosureRedisMySqlIntegrationTest {

    private static final Instant AS_OF =
            Instant.parse("2026-08-26T05:04:00Z");
    private static final String CHANNEL =
            CampaignLifecycleRedisProperties.DEFAULT_CHANNEL;

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
                    .withExposedPorts(6379);

    static {
        REDIS.start();
    }

    private static LettuceConnectionFactory subscriberConnectionFactory;
    private static RedisMessageListenerContainer subscriberContainer;
    private static List<Long> received;

    private final JdbcTemplate jdbc;
    private final CouponRoundLifecycleService lifecycleService;
    private final TransactionTemplate transactionTemplate;

    CampaignClosureRedisMySqlIntegrationTest(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            CouponRoundLifecycleService lifecycleService,
            @Qualifier("transactionManager")
            PlatformTransactionManager transactionManager
    ) {
        this.jdbc = jdbc;
        this.lifecycleService = lifecycleService;
        this.transactionTemplate = new TransactionTemplate(
                transactionManager
        );
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry properties) {
        properties.add("spring.data.redis.host", REDIS::getHost);
        properties.add("spring.data.redis.port", () ->
                REDIS.getMappedPort(6379));
    }

    @BeforeAll
    static void startSubscriber() {
        received = new CopyOnWriteArrayList<>();
        subscriberConnectionFactory = new LettuceConnectionFactory(
                REDIS.getHost(),
                REDIS.getMappedPort(6379)
        );
        subscriberConnectionFactory.afterPropertiesSet();
        subscriberConnectionFactory.start();
        subscriberContainer = new RedisMessageListenerContainer();
        subscriberContainer.setConnectionFactory(
                subscriberConnectionFactory
        );
        subscriberContainer.addMessageListener(
                new RedisCampaignClosedEventSubscriber(
                        JsonMapper.builder().findAndAddModules().build(),
                        (campaignCouponId, closedAt) ->
                                received.add(campaignCouponId)
                ),
                new ChannelTopic(CHANNEL)
        );
        subscriberContainer.afterPropertiesSet();
        subscriberContainer.start();
    }

    @AfterAll
    static void stopSubscriberAndRedis() throws Exception {
        if (subscriberContainer != null) {
            subscriberContainer.destroy();
        }
        if (subscriberConnectionFactory != null) {
            subscriberConnectionFactory.destroy();
        }
        REDIS.stop();
    }

    @BeforeEach
    void resetDatabase() {
        jdbc.update("DELETE FROM coupon_stocks");
        jdbc.update("DELETE FROM coupons");
        jdbc.update("DELETE FROM coupon_templates");
        jdbc.update("DELETE FROM brands");
        jdbc.update("INSERT INTO brands(id, name, category) VALUES (1, 'brand', 'CAFE')");
        jdbc.update("""
                INSERT INTO coupon_templates(
                    id, brand_id, name, policy_type, valid_days,
                    nth_week, day_of_week, start_time, duration_hours,
                    stock_per_occurrence, eligible_grades_mask, active,
                    created_at, updated_at
                ) VALUES (1, 1, 'template', 'FIXED_AMOUNT', 30,
                          1, 'MON', '10:00:00', 1,
                          100, 1, true, ?, ?)
                """, timestamp(AS_OF.minus(Duration.ofDays(2))),
                timestamp(AS_OF.minus(Duration.ofDays(2))));
        received.clear();
    }

    @Test
    @DisplayName("실제 MySQL 커밋 뒤에만 Redis 종료 메시지를 발행한다")
    void publishOnlyAfterMySqlCommit() {
        insertOpenCampaign(201L);

        lifecycleService.synchronize(AS_OF);

        assertThat(statusOf(201L)).isEqualTo("CLOSED");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(received).containsExactly(201L));
    }

    @Test
    @DisplayName("실제 MySQL 롤백이면 Redis 종료 메시지를 발행하지 않는다")
    void doNotPublishAfterMySqlRollback() {
        insertOpenCampaign(202L);

        transactionTemplate.executeWithoutResult(status -> {
            lifecycleService.synchronize(AS_OF);
            status.setRollbackOnly();
        });

        assertThat(statusOf(202L)).isEqualTo("OPEN");
        await().pollDelay(Duration.ofMillis(700))
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(received).isEmpty());
    }

    private void insertOpenCampaign(long id) {
        Instant openAt = AS_OF.minus(Duration.ofHours(2));
        Instant closeAt = AS_OF.minus(Duration.ofHours(1));
        jdbc.update("""
                INSERT INTO coupons(
                    id, template_id, brand_id, name, policy_type,
                    valid_days, eligible_grades_mask, open_at, close_at,
                    status, generated_at, created_at
                ) VALUES (?, 1, 1, ?, 'FIXED_AMOUNT',
                          30, 1, ?, ?, 'OPEN', ?, ?)
                """, id, "campaign-" + id,
                timestamp(openAt),
                timestamp(closeAt),
                timestamp(openAt.minusSeconds(1)),
                timestamp(openAt.minusSeconds(1)));
    }

    private String statusOf(long id) {
        return jdbc.queryForObject(
                "SELECT status FROM coupons WHERE id = ?",
                String.class,
                id
        );
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.from(instant);
    }
}
