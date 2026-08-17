package com.kafkick.core.admin.overview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.Severity;

/** 운영 현황 Provider 계약이 Core 경계에서 안전하게 사용되는지 검증합니다. */
class AdminOverviewCoreContractTest {

    private static final Instant FROM = Instant.parse("2026-08-17T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-08-17T01:00:00Z");

    /** Provider가 API 타입에 의존하지 않고 Core 조회·결과 타입만 사용하는지 검증합니다. */
    @Test
    void providerUsesCoreQueryAndSnapshotTypes() throws Exception {
        Method method = AdminOverviewProvider.class.getDeclaredMethod("getOverview", AdminOverviewQuery.class);

        assertThat(method.getReturnType()).isEqualTo(AdminOverviewSnapshot.class);
        assertThat(AdminOverviewProvider.class.getDeclaredMethods()).containsExactly(method);
    }

    /** 생성 후 원본 Set 변경이 조회 조건을 바꾸는 회귀를 방지합니다. */
    @Test
    void queryDefensivelyCopiesCouponIds() {
        Set<Long> couponIds = new HashSet<>(Set.of(11L));

        AdminOverviewQuery query = new AdminOverviewQuery(FROM, TO, couponIds);
        couponIds.add(22L);

        assertThat(query.couponIds()).containsExactly(11L);
    }

    /** Query가 반환한 쿠폰 집합을 호출자가 변경하지 못하게 합니다. */
    @Test
    void queryExposesUnmodifiableCouponIds() {
        AdminOverviewQuery query = new AdminOverviewQuery(FROM, TO, new HashSet<>(Set.of(11L)));

        assertThatThrownBy(() -> query.couponIds().add(22L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** 미지정 필터를 뜻하는 null 계약은 방어적 복사 후에도 유지합니다. */
    @Test
    void queryPreservesNullCouponIds() {
        assertThat(new AdminOverviewQuery(FROM, TO, null).couponIds()).isNull();
    }

    /** 조치 항목이 고객 영향 코드와 설명을 함께 보존하는지 검증합니다. */
    @Test
    void actionItemPreservesCustomerImpact() {
        AdminOverviewSnapshot.OperationActionItem item = new AdminOverviewSnapshot.OperationActionItem(
                1L,
                "캠페인",
                FROM,
                Severity.WARN,
                AdminOverviewSnapshot.CustomerImpact.LIMITED,
                "일부 고객 대기",
                FROM,
                Duration.ofMinutes(1),
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.QUEUE_STALLED,
                        "대기열 확인",
                        AdminOverviewSnapshot.TargetScreen.METRICS));

        assertThat(item.customerImpact()).isEqualTo(AdminOverviewSnapshot.CustomerImpact.LIMITED);
        assertThat(item.customerImpactText()).isEqualTo("일부 고객 대기");
    }
}
