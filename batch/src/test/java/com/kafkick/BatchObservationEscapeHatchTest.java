package com.kafkick;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * 관측을 끄면 batch 가 <b>떠야 한다</b>. 관측 계정이 없는 환경에서 검증 배치와 verify 트리거까지
 * 멈추면, 관측 전용 풀을 따로 뗀 이유와 정반대가 된다.
 *
 * <p>스위치를 env 로 열어 둔 것만으로는 부족했다 — {@code management.yml} 이 obs 헬스 그룹을
 * 선언하는데, 스위치를 끄면 그 그룹이 지목하는 기여자가 사라져 기동이 실패했다.
 * 그룹은 관측을 켠 JVM 에 storage 가 요구하므로 지울 수 없어, <b>멤버십 검증을 껐다</b>
 * ({@code management.endpoint.health.validate-group-membership: false}).
 * 그 대가로 batch 에서는 기여자 이름이 바뀌어도 런타임에 안 잡힌다 — 관측 풀 장애는
 * {@code app.observation.collect.last.success.epoch} 가 멈추는 것으로 본다.
 */
@SpringBootTest(properties = {
        "observation.datasource.enabled=false",
        "observation.domain-gauge.enabled=false"
})
@Import(MySqlContainerConfig.class)
class BatchObservationEscapeHatchTest {

    @Test
    @DisplayName("관측을 끄면 관측 계정 없이도 batch 가 기동한다")
    void contextLoadsWithObservationDisabled() {
    }
}
