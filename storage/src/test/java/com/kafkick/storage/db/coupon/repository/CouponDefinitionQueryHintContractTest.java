package com.kafkick.storage.db.coupon.repository;

import jakarta.persistence.QueryHint;

import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.repository.QueryHints;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 이 힌트의 단위는 계약이다. {@code org.hibernate.timeout} 은 <b>초</b> 단위라 최소값이 1초이고,
 * 호출자 예산(100ms)의 열 배다. 그 상태에서는 호출자가 물러난 뒤에도 로더 스레드와 Hikari
 * 커넥션이 최대 1초 더 붙잡혀 발급 경로의 커넥션을 잠식한다 — 인스턴스 풀은 3이다.
 *
 * <p>단위가 되돌아가도 앱은 정상 기동하고 로그도 없다. 부하 중 발급 지연으로만 드러난다.
 */
class CouponDefinitionQueryHintContractTest {

    @Test
    void boundsTheDefinitionQueryInMillisecondsNotSeconds() throws Exception {
        QueryHints hints = CouponRoundJpaRepository.class
                .getMethod("findV2CouponDefinitions")
                .getAnnotation(QueryHints.class);

        assertThat(hints).as("정의 질의에 시간 상한이 없으면 느린 DB 가 커넥션을 무한정 붙잡는다")
                .isNotNull();
        QueryHint hint = hints.value()[0];
        assertThat(hint.name()).isEqualTo("jakarta.persistence.query.timeout");
        assertThat(Integer.parseInt(hint.value()))
                .as("호출자 예산(100ms)보다 크되 같은 자릿수여야 한다")
                .isBetween(101, 999);
    }
}
