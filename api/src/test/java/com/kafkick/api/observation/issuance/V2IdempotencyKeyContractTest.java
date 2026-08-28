package com.kafkick.api.observation.issuance;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.ObjectProvider;

import com.kafkick.api.observation.ObservationIssuanceProperties;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.v2.CouponIssuanceRouter;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinitionCache;
import com.kafkick.core.coupon.v2.V2CouponIssueResult;
import com.kafkick.core.coupon.v2.V2CouponIssueService;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * v2 발급도 v1 과 <b>같은 멱등키 규약</b>을 지나야 한다.
 *
 * <p>v1 은 {@code CouponOperationExecutionService} 첫 줄에서 UUID v4 를 강제하는데 v2 는 그
 * 검증을 물려받지 못했다. 그 틈으로 재구성 마커({@code __rebuilt__})가 게이트까지 들어간다 —
 * 워밍업이 이미 발급받은 회원 자리에 그 문자열을 멱등키로 적어 두므로, 클라이언트가 같은
 * 값을 헤더에 넣으면 Lua 의 전체 비교가 <b>일치</b>로 판정해 {@code -6}(완료된 재시도)이 되고,
 * 그러면 DB 에 있지도 않은 멱등 레코드를 찾다가 500 이 된다.
 *
 * <p>재고도 1인1매도 안 깨지지만 가용성이 깨지고, 마커 값은 설계 문서에 공개돼 있어 추측할
 * 필요조차 없다.
 */
class V2IdempotencyKeyContractTest {

    private static final String REQUEST_ID = "request-1";
    private static final String VALID_KEY = "550e8400-e29b-41d4-a716-446655440000";
    private static final Instant AT = Instant.parse("2026-08-28T05:00:00Z");

    private V2CouponIssueService v2Service;
    private CouponIssueObservationCoordinator coordinator;

    @BeforeEach
    void setUp() {
        v2Service = mock(V2CouponIssueService.class);
        IssuanceObservationContextFactory contextFactory =
                mock(IssuanceObservationContextFactory.class);
        when(contextFactory.create(any(), anyLong(), anyLong(), any(), any()))
                .thenReturn(Optional.empty());
        coordinator = new CouponIssueObservationCoordinator(
                mock(CouponOperationExecutionService.class),
                contextFactory,
                mock(IssuanceObservationService.class),
                new CouponIssueObservationDependencyMapper(),
                v2Router(),
                v2ServiceProvider(),
                new V2IssuanceOutcomeMeters(new SimpleMeterRegistry()),
                new ObservationIssuanceProperties(null, "api-1", null, null),
                new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC))
        );
    }

    /**
     * 마커 문자열을 리터럴로 적었지만 <b>마커를 막는 테스트가 아니다.</b> 규약은 UUID v4
     * 허용목록이라 마커가 어떤 값으로 바뀌어도 같은 자리에서 막힌다 — 값을 바꾸는 것이
     * 해법이 아니라는 것이 이 테스트가 고정하는 사실이다.
     */
    @ParameterizedTest
    @ValueSource(strings = {"__rebuilt__", "", " ", "not-a-uuid",
            "550e8400-e29b-11d4-a716-446655440000"})
    void rejectsAnyIdempotencyKeyThatIsNotUuidV4(String idempotencyKey) {
        assertThatThrownBy(() -> issue(idempotencyKey))
                .isInstanceOf(BusinessException.class)
                .extracting(failure -> ((BusinessException) failure).getErrorCode())
                .isSameAs(CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST);
    }

    /** 게이트를 부르기 전에 막는다. 부르고 나면 그 자리에 선점이 남을 수 있다. */
    @Test
    void doesNotReachTheGateWithARejectedKey() {
        assertThatThrownBy(() -> issue("__rebuilt__")).isInstanceOf(BusinessException.class);

        verify(v2Service, never()).issue(any(), any());
    }

    @Test
    void letsAValidUuidThrough() {
        when(v2Service.issue(any(), any())).thenReturn(V2CouponIssueResult.replayed(
                ClaimResult.rejected(com.kafkick.core.coupon.v2.port.ClaimOutcome.REPLAY_DONE),
                issueResult()));

        assertThat(issue(VALID_KEY)).isEqualTo(issueResult());
    }

    private CouponIssueResult issue(String idempotencyKey) {
        return coordinator.issue(
                REQUEST_ID, 10L, 20L, MembershipGrade.GOLD, idempotencyKey);
    }

    private static CouponIssuanceRouter v2Router() {
        return new CouponIssuanceRouter(new CouponRoundIssuanceDefinitionCache(
                new CouponRoundIssuanceDefinitionRepository() {

                    @Override
                    public Optional<CouponRoundIssuanceDefinition> lockAndFindById(
                            long couponRoundId
                    ) {
                        return Optional.of(new CouponRoundIssuanceDefinition(
                                couponRoundId, 30, EngineVersion.V2));
                    }

                    @Override
                    public boolean updateEngineVersionWhenNotOpen(
                            long couponRoundId, EngineVersion engineVersion
                    ) {
                        throw new UnsupportedOperationException();
                    }
                }));
    }

    @SuppressWarnings("unchecked")
    private ObjectProvider<V2CouponIssueService> v2ServiceProvider() {
        ObjectProvider<V2CouponIssueService> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(v2Service);
        return provider;
    }

    private static CouponIssueResult issueResult() {
        return new CouponIssueResult(
                100L, 10L, "ABCDEFGHJKLM2345", IssuanceStatus.ISSUED,
                AT, AT.plusSeconds(604_800));
    }
}
