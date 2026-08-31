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
import com.kafkick.core.coupon.v2.V2CouponIssueException;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.member.Grade;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Dependency;
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
                new IssueLockRetryMeters(new SimpleMeterRegistry()),
                IssueLockRetryProperties.defaults(),
                new ObservationIssuanceProperties(null, "api-1", 3, 5, null),
                new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC))
        );
    }

    @ParameterizedTest(name = "{0} -> {1} {2}")
    @CsvSource({
            "CLOSED,             409, COUPON-303, COUPON_ROUND_CLOSED",
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
    void recordsRedisDatabaseStockDivergenceBeforeReturningSoldOut() {
        when(v2Service.issue(any(), any())).thenReturn(V2CouponIssueResult.rejectedAfterDatabaseSoldOut(CompensateOutcome.REVERTED));

        assertThatThrownBy(this::issue).isInstanceOf(BusinessException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_DATABASE_STOCK_DIVERGENCE)).isEqualTo(1);
    }

    @Test
    void mapsLostClaimOwnershipDuringCompensationToRetryableRedisUnavailableAndCountsIt() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("claim disappeared after failover"),
                CompensateOutcome.NOT_MINE, Dependency.REDIS));

        assertThatThrownBy(this::issue).isInstanceOfSatisfying(RetryAfterException.class, failure -> {
            assertThat(failure.getErrorCode()).isSameAs(CouponIssueV2ErrorCode.REDIS_UNAVAILABLE);
            assertThat(failure.retryAfterSeconds()).isEqualTo(1);
        });
        assertThat(count(MeterNames.ISSUANCE_V2_REDIS_UNAVAILABLE)).isEqualTo(1);
    }

    /**
     * 게이트가 통과시킨 중복은 409 그대로지만 전용 카운터가 함께 오른다. 게이트가 스스로
     * 거른 중복과 뭉치면 복제 유실이 평범한 재요청 물결에 묻힌다.
     */
    @Test
    void databaseCaughtDuplicateStaysA409AndRaisesTheMemberDivergenceCounter() {
        when(v2Service.issue(any(), any()))
                .thenReturn(V2CouponIssueResult.rejectedAfterDatabaseDuplicate(CompensateOutcome.REVERTED));

        assertThatThrownBy(this::issue).isInstanceOf(BusinessException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_DATABASE_MEMBER_DIVERGENCE)).isEqualTo(1);
        assertThat(count(MeterNames.ISSUANCE_V2_REDIS_UNAVAILABLE)).isZero();
    }

    /** 매진 괴리와 회원 괴리를 서로의 카운터에 넣지 않는다. */
    @Test
    void memberDivergenceAndStockDivergenceStaySeparate() {
        when(v2Service.issue(any(), any()))
                .thenReturn(V2CouponIssueResult.rejectedAfterDatabaseSoldOut(CompensateOutcome.REVERTED));
        assertThatThrownBy(this::issue).isInstanceOf(BusinessException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_DATABASE_MEMBER_DIVERGENCE)).isZero();
        assertThat(count(MeterNames.ISSUANCE_V2_DATABASE_STOCK_DIVERGENCE)).isEqualTo(1);
    }

    /**
     * 게이트가 <b>막지 못한</b> 중복은 dupPerMember 에 넣지 않는다. 그 카운터의 정의가
     * "1인1매 방어의 발동 빈도" 라, 게이트를 통과한 건을 섞으면 Sentinel 승격이
     * 봇 재요청 물결로 읽힌다.
     */
    @Test
    void databaseCaughtDuplicateStaysOutOfTheGateDuplicateCounter() {
        when(v2Service.issue(any(), any())).thenReturn(
                V2CouponIssueResult.rejectedAfterDatabaseDuplicate(CompensateOutcome.REVERTED));

        assertThatThrownBy(this::issue).isInstanceOf(BusinessException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_DUP_PER_MEMBER)).isZero();
        assertThat(count(MeterNames.ISSUANCE_V2_DATABASE_MEMBER_DIVERGENCE)).isEqualTo(1);
    }

    /** 되돌릴 선점이 없었다 — 다른 절차가 먼저 정리했다. 이 요청은 아무것도 남기지 않았다. */
    @Test
    void anAbsentClaimIsCountedApartFromTheLeak() {
        when(v2Service.issue(any(), any())).thenReturn(
                V2CouponIssueResult.rejectedAfterDatabaseDuplicate(CompensateOutcome.NO_CLAIM));

        assertThatThrownBy(this::issue).isInstanceOf(BusinessException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_COMPENSATION_NO_CLAIM)).isEqualTo(1);
        assertThat(count(MeterNames.ISSUANCE_V2_CLAIM_LEAKED)).isZero();
        assertThat(count(MeterNames.ISSUANCE_V2_DATABASE_MEMBER_DIVERGENCE)).isEqualTo(1);
    }

    /**
     * 남의 토큰이 내 선점을 덮었다 — {@code DECR} 이 복구되지 않는다. 예전에는 이것이
     * "없다" 와 같은 코드라 확정할 수 없었고, 그래서 한쪽에서는 정상, 다른 쪽에서는 누수로
     * 읽히는 일이 반복됐다. 갈라 놓았으므로 이제 <b>확정 누수</b>다.
     */
    @Test
    void aClaimOverwrittenByAnotherTokenIsADefiniteLeak() {
        when(v2Service.issue(any(), any())).thenReturn(
                V2CouponIssueResult.rejectedAfterDatabaseDuplicate(CompensateOutcome.NOT_MINE));

        assertThatThrownBy(this::issue).isInstanceOf(BusinessException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_CLAIM_LEAKED)).isEqualTo(1);
        assertThat(count(MeterNames.ISSUANCE_V2_COMPENSATION_NO_CLAIM)).isZero();
    }

    /** 남긴 것이 없으면 재시도를 권하지 않는다 — 응답과 계수가 같은 판정을 쓴다. */
    @Test
    void anAbsentClaimIsNotRetryable() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("nothing to revert"),
                CompensateOutcome.NO_CLAIM, Dependency.REDIS));

        assertThatThrownBy(this::issue).isNotInstanceOf(RetryAfterException.class);
    }

    /** 보상을 보내지도 못한 것은 선점이 남은 것이 확실하다 — 이쪽이 누수다. */
    @Test
    void unsentCompensationOnADatabaseRejectionIsCountedAsALeak() {
        when(v2Service.issue(any(), any())).thenReturn(
                V2CouponIssueResult.rejectedAfterDatabaseSoldOut(
                        CompensateOutcome.NOT_ATTEMPTED_CIRCUIT_OPEN));

        assertThatThrownBy(this::issue).isInstanceOf(BusinessException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_CLAIM_LEAKED)).isEqualTo(1);
        assertThat(count(MeterNames.ISSUANCE_V2_REDIS_UNAVAILABLE)).isZero();
    }

    /**
     * DB 실패 경로에서 보상이 깨진 것도 503 이다. 의존성으로 거르면 이 경로만
     * 재시도 안내 없는 500 으로 새어 나간다.
     */
    @Test
    void aBrokenCompensationOnTheDatabasePathIsAlsoRetryable() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("insert failed"),
                CompensateOutcome.ATTEMPT_FAILED, Dependency.MYSQL));

        assertThatThrownBy(this::issue).isInstanceOfSatisfying(RetryAfterException.class, failure ->
                assertThat(failure.getErrorCode()).isSameAs(CouponIssueV2ErrorCode.REDIS_UNAVAILABLE));
    }

    /** 되돌린 것이 확실하면 누수가 아니다. */
    @Test
    void revertedClaimIsNotCountedAsALeak() {
        when(v2Service.issue(any(), any())).thenReturn(
                V2CouponIssueResult.rejectedAfterDatabaseSoldOut(CompensateOutcome.REVERTED));

        assertThatThrownBy(this::issue).isInstanceOf(BusinessException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_CLAIM_LEAKED)).isZero();
    }

    /**
     * 보상 호출이 깨진 경우가 가장 불확실한데, 여기가 비어 있으면 재시도 안내 없는 500 으로
     * 새어 나가고 Redis 장애 구간에 스택 트레이스가 폭주한다.
     */
    @Test
    void failedCompensationAttemptBecomesRetryableRedisUnavailable() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("compensate threw"),
                CompensateOutcome.ATTEMPT_FAILED, Dependency.REDIS));

        assertThatThrownBy(this::issue).isInstanceOfSatisfying(RetryAfterException.class, failure ->
                assertThat(failure.getErrorCode()).isSameAs(CouponIssueV2ErrorCode.REDIS_UNAVAILABLE));
    }

    /**
     * 반대 방향의 과교정을 막는다. 보상 결과가 <b>아예 없는</b> 것은 발급이 이미 커밋돼
     * 보상하지 않기로 한 경로다 — 재시도를 권하면 쿠폰을 받은 사람이 다시 누른다.
     */
    @Test
    void absentCompensationOutcomeIsNotTurnedIntoARetrySuggestion() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("완료 CAS 가 비정상 결과를 냈습니다"),
                null, Dependency.REDIS));

        assertThatThrownBy(this::issue).isNotInstanceOf(RetryAfterException.class);
    }

    /**
     * 되돌아오지 않은 선점은 대부분 예외로 나간다 — 보상 결과가 결과에 실리는 것은 DB 확정 경로뿐이라 나머지는
     * 값만 실리기 때문이다. 예외 경로에서 세지 않으면 재고가 확실히 낮아진 건이 0 으로 보인다.
     */
    @Test
    void countsTheLeakEvenWhenTheClaimEscapesAsAnException() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("compensate threw"),
                CompensateOutcome.ATTEMPT_FAILED, Dependency.REDIS));

        assertThatThrownBy(this::issue).isInstanceOf(RuntimeException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_CLAIM_LEAKED)).isEqualTo(1);
    }

    /** 차단기가 열려 보상을 보내지도 못한 건도 누수다. */
    @Test
    void countsTheLeakWhenCompensationWasNeverSent() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("circuit open"),
                CompensateOutcome.NOT_ATTEMPTED_CIRCUIT_OPEN, Dependency.REDIS));

        assertThatThrownBy(this::issue).isInstanceOf(RuntimeException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_CLAIM_LEAKED)).isEqualTo(1);
    }

    /** 되돌린 것이 확실하면 예외로 나가도 누수가 아니다. */
    @Test
    void doesNotCountALeakWhenCompensationReverted() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("something else"),
                CompensateOutcome.REVERTED, Dependency.REDIS));

        assertThatThrownBy(this::issue).isInstanceOf(RuntimeException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_CLAIM_LEAKED)).isZero();
    }

    /**
     * 선점이 남은 것이 <b>확정</b>인 값이 불확정 누수보다 약한 응답을 받으면 안 된다.
     * {@code COUNTER_UNREADABLE} 은 "되돌리기 전에 본 카운터를 못 읽어 아무것도 적용되지
     * 않았다" — 재시도 안내 없는 500 으로 내보내면 클라이언트는 이탈하고 선점은 남는다.
     */
    @Test
    void aCompensationThatCouldNotReadCountersIsRetryable() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("counters unreadable"),
                CompensateOutcome.COUNTER_UNREADABLE, Dependency.REDIS));

        assertThatThrownBy(this::issue).isInstanceOfSatisfying(RetryAfterException.class, failure ->
                assertThat(failure.getErrorCode()).isSameAs(CouponIssueV2ErrorCode.REDIS_UNAVAILABLE));
    }

    /** 이미 완료 승격된 선점은 되돌릴 것이 없다 — 재시도를 권하지도, 누수로 세지도 않는다. */
    @Test
    void compensationOnACompletedClaimIsNeitherRetryableNorALeak() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("already promoted"),
                CompensateOutcome.ALREADY_DONE, Dependency.REDIS));

        assertThatThrownBy(this::issue).isNotInstanceOf(RetryAfterException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_COMPENSATION_ALREADY_DONE)).isEqualTo(1);
        assertThat(count(MeterNames.ISSUANCE_V2_CLAIM_LEAKED)).isZero();
    }

    /**
     * <b>재고 사실과 응답 정책이 갈리는 유일한 값이다.</b> 거부한 것은 <b>보상</b> 스크립트라
     * 선점은 이미 성립해 있고 {@code HDEL}·{@code INCR} 이 하나도 안 돌았다 — 그러므로
     * {@code claim.leaked} 로 <b>센다</b>. 다만 호출부 버그라 다시 눌러도 같은 실패이므로
     * 재시도는 권하지 않는다. 목록은 {@code leftClaimBehind} 하나뿐이고 여기서만 이름 있는
     * 예외로 뺀다.
     */
    @Test
    void aBadArgumentCompensationIsCountedAsALeakButNotRetryable() {
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("bad argv"),
                CompensateOutcome.BAD_ARGUMENT, Dependency.REDIS));

        assertThatThrownBy(this::issue).isNotInstanceOf(RetryAfterException.class);

        assertThat(count(MeterNames.ISSUANCE_V2_CLAIM_LEAKED)).isEqualTo(1);
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
            "CLOSED,             409, COUPON-303, COUPON_ROUND_CLOSED",
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

    /**
     * <b>응답을 503 으로 옮겨도 귀속은 원 실패에서 온다.</b> 변환된 예외를 매퍼에 넘기면
     * {@code REDIS_UNAVAILABLE} 의 {@code Dependency.REDIS} 가 관측에 박혀, MySQL 장애가
     * Redis 장애로 집계된다 — Chaos 리포트가 "MySQL 주입 구간에 Redis 장애 급증" 으로
     * 뒤집혀 읽힌다. 어댑터가 확정해 실어 보낸 dependency 가 응답 매핑에 덮이면 안 된다.
     */
    @Test
    void aDatabaseFailureStaysAttributedToMysqlEvenWhenTheResponseBecomesRedisUnavailable() {
        List<IssuanceFlowEvent> events = new CopyOnWriteArrayList<>();
        when(v2Service.issue(any(), any())).thenThrow(new V2CouponIssueException(
                new IllegalStateException("insert failed"),
                CompensateOutcome.ATTEMPT_FAILED, Dependency.MYSQL));

        assertThatThrownBy(() -> observingCoordinator(events).issue(
                REQUEST_ID, 10L, 20L, MembershipGrade.GOLD, IDEMPOTENCY_KEY))
                .isInstanceOf(RetryAfterException.class);

        IssuanceFlowEvent result = events.stream()
                .filter(event -> event.eventType() == EventType.ISSUE_RESULT)
                .findFirst()
                .orElseThrow();
        assertThat(result.httpStatus()).isEqualTo(503);
        assertThat(result.dependency())
                .as("응답은 503 이라도 실제로 막힌 것은 MySQL 이다")
                .isEqualTo(Dependency.MYSQL);
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
                new IssueLockRetryMeters(new SimpleMeterRegistry()),
                IssueLockRetryProperties.defaults(),
                new ObservationIssuanceProperties(null, "api-1", 3, 5, null),
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
                    public Optional<CouponRoundIssuanceDefinition> findById(
                            long couponRoundId
                    ) {
                        // 발급 경로는 lockAndFindById 만 쓴다. 여기로 오면 배선이 틀린 것이다.
                        throw new UnsupportedOperationException();
                    }

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
