package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class NotificationRelayPropertiesTest {
    @Test
    void defaultsMatchRelayContract() {
        NotificationRelayProperties properties = new NotificationRelayProperties();

        assertThat(properties.getLease()).isEqualTo(Duration.ofSeconds(30));
        assertThat(properties.getFixedDelayMs()).isEqualTo(100L);
        assertThat(properties.getClaimBatchSize()).isEqualTo(64);
        assertThat(properties.getMaxInFlight()).isEqualTo(64);
        assertThat(properties.getWorkerCount()).isEqualTo(8);
    }

    /**
     * <b>기본값이 릴레이 생성자를 통과해야 한다.</b> 이 둘은 파일이 갈라져 있어서
     * 각각으로는 못 지킨다 — 기본값을 고치다 lease 검사에 걸리는 조합을 넣으면
     * <b>아무 설정도 안 한 배포가 기동에 실패한다.</b>
     *
     * <p>{@code ceil(64 / 8) × 100ms = 800ms} 로 30초 lease 안쪽이다.
     */
    @Test
    void theDefaultsPassTheRelaysOwnStartupCheck() {
        NotificationRelayProperties properties = new NotificationRelayProperties();

        int waves = (properties.getMaxInFlight() + properties.getWorkerCount() - 1)
                / properties.getWorkerCount();
        assertThat(Duration.ofMillis(waves * 100L))
                .as("기본값이 lease 검사에 걸리면 기본 배포가 기동을 못 합니다")
                .isLessThan(properties.getLease());
    }

    /** 0 이면 백프레셔가 항상 걸려 릴레이가 <b>아무것도 집지 않고 조용히 정상으로 보인다.</b> */
    @Test
    void rejectsInFlightBoundOutsideTheSupportedRange() {
        NotificationRelayProperties properties = new NotificationRelayProperties();

        assertThatThrownBy(() -> properties.setMaxInFlight(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties.setMaxInFlight(1_001))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 0 이면 풀이 아무것도 실행하지 못해 인플라이트가 상한에 붙은 채 굳는다.
     * 위쪽 상한은 스레드 비용이다 — 이 풀은 접수 API 프로세스 안에서 돈다.
     */
    @Test
    void rejectsWorkerCountOutsideTheSupportedRange() {
        NotificationRelayProperties properties = new NotificationRelayProperties();

        assertThatThrownBy(() -> properties.setWorkerCount(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties.setWorkerCount(1_001))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 집은 수만큼 id·UUID 를 메모리에 만들고 같은 수의 {@code IN} 자리표시자를 붙인다.
     * 백로그가 큰 상태에서 큰 값을 주면 릴레이가 그것 때문에 죽는다.
     */
    @Test
    void rejectsBatchSizeOutsideTheSupportedRange() {
        NotificationRelayProperties properties = new NotificationRelayProperties();

        assertThatThrownBy(() -> properties.setClaimBatchSize(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties.setClaimBatchSize(1_001))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
