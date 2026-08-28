package com.kafkick.api.observation.issuance;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.ObjectProvider;

import com.kafkick.api.observation.MeterNames;
import com.kafkick.api.observation.ObservationIssuanceProperties;
import com.kafkick.api.support.RetryAfterException;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponIssueV2ErrorCode;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.v2.CouponIssuanceRouter;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinitionCache;
import com.kafkick.core.coupon.v2.V2CouponIssueResult;
import com.kafkick.core.coupon.v2.V2CouponIssueService;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.member.Grade;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.ErrorCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * v2 게이트 반환의 HTTP·지표 매핑. {@code -4}·{@code -6}·{@code -7} 이 서로 다른 응답과
 * 서로 다른 카운터로 갈리는지가 이 테스트의 본체다.
 */
class V2IssueResponseMappingTest {

    private static final String REQUEST_ID = "request-1";
    private static final String IDEMPOTENCY_KEY = "550e8400-e29b-41d4-a716-446655440000";
    private static final Instant AT = Instant.parse("2026-08-28T05:00:00Z");

    private V2CouponIssueService v2Service;
    private SimpleMeterRegistry meterRegistry;
    private V2IssuanceOutcomeMeters meters;
    private CouponIssueObservationCoordinator coordinator;

    @BeforeEach
    void setUp() {
        v2Service = mock(V2CouponIssueService.class);
        meterRegistry = new SimpleMeterRegistry();
        meters = new V2IssuanceOutcomeMeters(meterRegistry);
        IssuanceObservationContextFactory contextFactory =
                mock(IssuanceObservationContextFactory.class);
        when(contextFactory.create(any(), anyLong(), anyLong(), any(), any()))
                .thenReturn(Optional.empty());
        coordinator = new CouponIssueObservationCoordinator(
                mock(com.kafkick.core.coupon.service.CouponOperationExecutionService.class),
                contextFactory,
                mock(IssuanceObservationService.class),
                new CouponIssueObservationDependencyMapper(),
                v2Router(),
                v2ServiceProvider(),
                meters,
                new ObservationIssuanceProperties(null, "api-1", 3, 5),
                new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC))
        );
    }

    @ParameterizedTest(name = "{0} -> {1} {2}")
    @CsvSource({
            "CLOSED,             409, COUPON-303, CAMPAIGN_CLOSED",
            "NOT_OPEN,           409, COUPON-302, NOT_OPENED",
            "GRADE_NOT_ALLOWED,  403, COUPON-304, GRADE_NOT_ELIGIBLE",
            "DUP_PER_MEMBER,     409, COUPON-305, ALREADY_ISSUED",
            "SOLD_OUT,           409, COUPON-306, STOCK_EXHAUSTED",
            "REPLAY_PENDING,     409, COUPON-320, REPLAY_IN_PROGRESS",
            "CORRUPT_VALUE,      500, COUPON-321, VALUE_CORRUPT",
            "GATE_NOT_READY,     503, COUPON-322, GATE_NOT_READY",
            "BAD_ARGUMENT,       500, COUPON-323, BAD_ARGUMENT",
            "COUNTER_UNREADABLE, 503, COUPON-324, COUNTER_UNREADABLE"
    })
    void mapsEveryRejectionToItsOwnStatusAndReason(
            ClaimOutcome outcome, int status, String code, ReasonCode reasonCode
    ) {
        rejects(outcome);

        ErrorCode actual = businessFailure().getErrorCode();

        assertThat(actual.getStatus()).isEqualTo(status);
        assertThat(actual.getCode()).isEqualTo(code);
        assertThat(actual.reasonCode()).contains(reasonCode);
    }

    /** 음수 반환을 삼키는 경로가 없다 — 모든 거절이 매핑에 도달한다. */
    @ParameterizedTest
    @EnumSource(value = ClaimOutcome.class, names = {"CLAIMED", "REPLAY_DONE"},
            mode = EnumSource.Mode.EXCLUDE)
    void everyRejectionReachesTheMapping(ClaimOutcome outcome) {
        rejects(outcome);

        Optional<ReasonCode> reasonCode = businessFailure().getErrorCode().reasonCode();

        assertThat(reasonCode).isPresent();
        assertThat(reasonCode).isNotEqualTo(Optional.of(ReasonCode.UNMAPPED));
    }

    @Test
    void duplicatePerMemberAndReplayAreNotTheSameResponse() {
        rejects(ClaimOutcome.DUP_PER_MEMBER);
        ErrorCode duplicate = businessFailure().getErrorCode();

        when(v2Service.issue(any(), any())).thenReturn(V2CouponIssueResult.replayed(
                ClaimResult.rejected(ClaimOutcome.REPLAY_DONE), issueResult()));
        CouponIssueResult replayed = issue();

        rejects(ClaimOutcome.REPLAY_PENDING);
        ErrorCode pending = businessFailure().getErrorCode();

        assertThat(replayed).isEqualTo(issueResult());
        assertThat(duplicate.getCode()).isNotEqualTo(pending.getCode());
        assertThat(duplicate.reasonCode()).isNotEqualTo(pending.reasonCode());
    }

    @Test
    void replayPendingCarriesRetryAfterAndReplayDoneDoesNot() {
        rejects(ClaimOutcome.REPLAY_PENDING);

        assertThat(businessFailure()).isInstanceOf(RetryAfterException.class);
        // 리터럴이 아니라 설정값이다. 반영에는 api 재기동이 필요하다.
        assertThat(((RetryAfterException) businessFailure()).retryAfterSeconds()).isEqualTo(3);
    }

    @Test
    void gateNotReadyRetriesAndCounterUnreadableDoesNot() {
        rejects(ClaimOutcome.GATE_NOT_READY);
        assertThat(businessFailure()).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) businessFailure()).retryAfterSeconds()).isEqualTo(5);

        rejects(ClaimOutcome.COUNTER_UNREADABLE);
        assertThat(businessFailure()).isNotInstanceOf(RetryAfterException.class);
    }

    @Test
    void separatesTheThreeCounters() {
        rejects(ClaimOutcome.DUP_PER_MEMBER);
        businessFailure();
        rejects(ClaimOutcome.REPLAY_PENDING);
        businessFailure();
        businessFailure();
        when(v2Service.issue(any(), any())).thenReturn(V2CouponIssueResult.replayed(
                ClaimResult.rejected(ClaimOutcome.REPLAY_DONE), issueResult()));
        issue();

        assertThat(count(MeterNames.ISSUANCE_V2_DUP_PER_MEMBER)).isEqualTo(1);
        assertThat(count(MeterNames.ISSUANCE_V2_REPLAY_PENDING)).isEqualTo(2);
        assertThat(count(MeterNames.ISSUANCE_V2_REPLAY_DONE)).isEqualTo(1);
    }

    /** 매진과 카운터 미판독을 같은 카운터로 뭉치지 않는다. */
    @Test
    void soldOutDoesNotTouchTheDuplicateCounter() {
        rejects(ClaimOutcome.SOLD_OUT);
        businessFailure();

        assertThat(count(MeterNames.ISSUANCE_V2_DUP_PER_MEMBER)).isZero();
    }

    @Test
    void corruptValueIsNotSwallowedAsBadRequest() {
        rejects(ClaimOutcome.CORRUPT_VALUE);

        assertThat(businessFailure().getErrorCode())
                .isSameAs(CouponIssueV2ErrorCode.VALUE_CORRUPT);
    }

    /**
     * 관측 이벤트까지 실제로 나가는지 본다. 오류 코드 enum 만 보면 {@code dependencyMapper} →
     * {@code completeRejected} 배선이 끊겨 {@code UNMAPPED} 가 나가도 초록이다 — 완료 조건은
     * enum 정의가 아니라 {@code issue_attempts.reason_code} 에 대한 진술이다.
     */
    @ParameterizedTest(name = "{0} -> {1} {3}")
    @CsvSource({
            "CLOSED,             409, COUPON-303, CAMPAIGN_CLOSED",
            "NOT_OPEN,           409, COUPON-302, NOT_OPENED",
            "GRADE_NOT_ALLOWED,  403, COUPON-304, GRADE_NOT_ELIGIBLE",
            "DUP_PER_MEMBER,     409, COUPON-305, ALREADY_ISSUED",
            "SOLD_OUT,           409, COUPON-306, STOCK_EXHAUSTED",
            "REPLAY_PENDING,     409, COUPON-320, REPLAY_IN_PROGRESS",
            "CORRUPT_VALUE,      500, COUPON-321, VALUE_CORRUPT",
            "GATE_NOT_READY,     503, COUPON-322, GATE_NOT_READY",
            "BAD_ARGUMENT,       500, COUPON-323, BAD_ARGUMENT",
            "COUNTER_UNREADABLE, 503, COUPON-324, COUNTER_UNREADABLE"
    })
    void emitsTheRejectionReasonOnTheObservationEvent(
            ClaimOutcome outcome, int status, String unusedCode, ReasonCode reasonCode
    ) {
        List<IssuanceFlowEvent> events = new CopyOnWriteArrayList<>();
        rejects(outcome);

        assertThatThrownBy(() -> observingCoordinator(events).issue(
                REQUEST_ID, 10L, 20L, MembershipGrade.GOLD, IDEMPOTENCY_KEY))
                .isInstanceOf(BusinessException.class);

        IssuanceFlowEvent result = events.stream()
                .filter(event -> event.eventType() == EventType.ISSUE_RESULT)
                .findFirst()
                .orElseThrow();
        assertThat(result.reasonCode()).isEqualTo(reasonCode);
        assertThat(result.reasonCode()).isNotEqualTo(ReasonCode.UNMAPPED);
        assertThat(result.httpStatus()).isEqualTo(status);
    }

    /** 관측 기록기까지 진짜인 코디네이터. 발행된 이벤트를 {@code events} 에 모은다. */
    private CouponIssueObservationCoordinator observingCoordinator(
            List<IssuanceFlowEvent> events
    ) {
        TimeProvider timeProvider = new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC));
        IssuanceObservationContextFactory contextFactory =
                mock(IssuanceObservationContextFactory.class);
        when(contextFactory.create(any(), anyLong(), anyLong(), any(), any()))
                .thenReturn(Optional.of(new IssuanceFlowEvent.Ctx(
                        REQUEST_ID, 20L, 10L, Grade.GOLD, false, AT,
                        EngineVersion.V2, ReleaseStage.V2_1, QueueMode.OFF, null, "api-1")));
        return new CouponIssueObservationCoordinator(
                mock(com.kafkick.core.coupon.service.CouponOperationExecutionService.class),
                contextFactory,
                new IssuanceObservationService(
                        new IssuanceFlowEventFactory(() -> UUID.fromString(
                                "11111111-1111-1111-1111-111111111111")),
                        events::add,
                        timeProvider),
                new CouponIssueObservationDependencyMapper(),
                v2Router(),
                v2ServiceProvider(),
                meters,
                new ObservationIssuanceProperties(null, "api-1", 3, 5),
                timeProvider
        );
    }

    private double count(String name) {
        return meterRegistry.get(name).counter().count();
    }

    private void rejects(ClaimOutcome outcome) {
        when(v2Service.issue(any(), any())).thenReturn(
                V2CouponIssueResult.rejected(ClaimResult.rejected(outcome)));
    }

    private BusinessException businessFailure() {
        return (BusinessException) org.assertj.core.api.Assertions
                .catchThrowableOfType(this::issue, BusinessException.class);
    }

    private CouponIssueResult issue() {
        return coordinator.issue(
                REQUEST_ID, 10L, 20L, MembershipGrade.GOLD, IDEMPOTENCY_KEY);
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
