package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.kafkick.core.notification.NotificationSender;

/**
 * <b>발송기가 정확히 하나 뜬다.</b>
 *
 * <p>둘 다 뜨면 스프링이 주입에서 죽고, 하나도 안 뜨면 소비자가 기동에서 죽는다.
 * 어느 쪽도 <b>스위치를 잘못 준 환경에서만</b> 드러나므로 여기서 미리 태운다.
 *
 * <h2>{@code @ConditionalOnProperty} 를 두 개 겹친 것이 정말 AND 인가</h2>
 *
 * <p>{@link MockNotificationSender} 는 조건이 둘이다 — {@code kafka.enabled} 가 켜져 있고
 * {@code notification.sender.http.enabled} 가 <b>안 켜져 있을 때</b>. 애노테이션을 겹쳐
 * 쓴 것이 컴파일된다고 해서 <b>런타임에 AND 로 도는 것은 아니다</b>(하나가 조용히 무시되면
 * 두 발송기가 함께 뜬다). 그것을 여기서 실제 컨텍스트로 확인한다.
 */
class NotificationSenderWiringTest {

    @Configuration(proxyBeanMethods = false)
    @Import({MockNotificationSender.class, HttpNotificationSender.class})
    static class Senders {
    }

    /**
     * {@code @Value} 로 {@link java.time.Duration} 을 받으므로 <b>치환기와 변환 서비스가
     * 둘 다 있어야 한다.</b> 없으면 조건 판정 전에 생성자 주입이 실패해서, 이 테스트가
     * <b>배선이 아니라 컨테이너 설정 부재를 재게 된다</b> — 실제로 두 번 그렇게 빨개졌다
     * (치환기 없음 → {@code 50ms} 를 {@code Duration} 으로 못 바꿈).
     */
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PropertyPlaceholderAutoConfiguration.class))
            .withInitializer(context -> context.getBeanFactory()
                    .setConversionService(ApplicationConversionService.getSharedInstance()))
            .withUserConfiguration(Senders.class)
            .withPropertyValues("notification.sender.http.endpoint=http://notify.test/send");

    @Test
    @DisplayName("스위치를 안 켜면 Mock 하나만 뜬다 — 설정을 빠뜨려도 기동은 된다")
    void mockStandsWhenTheSwitchIsAbsent() {
        runner.withPropertyValues("kafka.enabled=true").run(context -> {
            assertThat(context).hasSingleBean(NotificationSender.class);
            assertThat(context.getBean(NotificationSender.class))
                    .isInstanceOf(MockNotificationSender.class);
        });
    }

    /**
     * <b>여기가 두 조건이 AND 인지 재는 자리다.</b> 스위치를 켰는데 Mock 이 함께 뜨면
     * {@code hasSingleBean} 이 깨진다 — 겹친 애노테이션 하나가 무시된 상태다.
     */
    @Test
    @DisplayName("스위치를 켜면 실제 발송기 하나만 뜬다 — Mock 이 함께 뜨지 않는다")
    void httpSenderReplacesTheMockWhenSwitchedOn() {
        runner.withPropertyValues("kafka.enabled=true",
                        "notification.sender.http.enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(NotificationSender.class);
                    assertThat(context.getBean(NotificationSender.class))
                            .isInstanceOf(HttpNotificationSender.class);
                });
    }

    /** {@code false} 를 명시해도 Mock 이다 — 켜고 끄기가 대칭이어야 한다. */
    @Test
    @DisplayName("스위치를 false 로 명시해도 Mock 이다")
    void mockStandsWhenTheSwitchIsExplicitlyOff() {
        runner.withPropertyValues("kafka.enabled=true",
                        "notification.sender.http.enabled=false")
                .run(context -> assertThat(context.getBean(NotificationSender.class))
                        .isInstanceOf(MockNotificationSender.class));
    }

    /**
     * <b>애노테이션이 정말 둘 붙어 있는지</b>도 본다. 위 테스트들은 조건이 하나만 남아도
     * 통과할 수 있다 — {@code kafka.enabled} 를 지워도 러너가 그것을 항상 주기 때문이다.
     */
    @Test
    @DisplayName("Mock 이 조건 둘을 다 들고 있다")
    void theMockDeclaresBothConditions() {
        ConditionalOnProperty[] conditions =
                MockNotificationSender.class.getAnnotationsByType(ConditionalOnProperty.class);

        assertThat(conditions)
                .as("하나만 남으면 kafka 를 끈 회차나 스위치를 켠 회차에서 발송기가 둘이 된다")
                .hasSize(2);
    }
}
