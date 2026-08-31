package com.kafkick.api.observation.issuance;

import java.time.Instant;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.kafkick.api.observation.ObservationIssuanceProperties;

import org.springframework.dao.CannotAcquireLockException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataIntegrityViolationException;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponAlreadyIssuedException;
import com.kafkick.core.coupon.exception.CouponIssueMemberNotFoundException;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;
import com.kafkick.core.coupon.service.IssueAttemptCallback;
import com.kafkick.core.coupon.service.result.CouponIssueExecutionResult;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.v2.CouponIssuanceRouter;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinitionCache;
import com.kafkick.core.coupon.v2.V2CouponIssueService;
import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.member.Grade;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponIssueObservationCoordinatorTest {

    private static final String REQUEST_ID = "request-1";
    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final Instant AT = Instant.parse("2026-08-24T05:00:00Z");

    @Mock
    private CouponOperationExecutionService operationExecutionService;
    @Mock
    private IssuanceObservationContextFactory contextFactory;
    @Mock
    private IssuanceObservationService observationService;
    @Mock
    private IssuanceObservationSession session;

    private CouponIssueObservationCoordinator coordinator;
    private IssuanceFlowEvent.Ctx context;

    @BeforeEach
    void setUp() {
        coordinator = new CouponIssueObservationCoordinator(
                operationExecutionService,
                contextFactory,
                observationService,
                new CouponIssueObservationDependencyMapper(),
                v1Router(),
                noV2Service(),
                new V2IssuanceOutcomeMeters(new SimpleMeterRegistry()),
                new IssueLockRetryMeters(new SimpleMeterRegistry()),
                IssueLockRetryProperties.defaults(),
                new ObservationIssuanceProperties(null, "api-1", null, null, null),
                new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC))
        );
        context = new IssuanceFlowEvent.Ctx(
                REQUEST_ID,
                20L,
                10L,
                Grade.GOLD,
                false,
                AT,
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.ADAPTIVE,
                901L,
                "api-1"
        );
    }

    /**
     * <b>동시 발급이 같은 회차에 몰리면 MySQL 이 한쪽을 데드락으로 걷어낸다.</b> 실측했다 —
     * 저장소 동시성 테스트에서 재시도를 빼고 여섯 번 돌리면 한 번 실패한다. 다시 시도하지
     * 않으면 그 요청은 사용자에게 500 이고 부하 회차의 에러율에 그대로 얹힌다.
     *
     * <p>발급·이력·재고·멱등 기록이 한 트랜잭션이라 롤백이 넷을 전부 되돌린다. 그래서 다시
     * 시도해도 막히지 않고 중복도 안 생긴다.
     */
    @Test
    @DisplayName("락 경합으로 물러선 발급은 다시 시도한다")
    void retriesIssueWhenLockCannotBeAcquired() {
        prepareContext();
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD), eq(IDEMPOTENCY_KEY), any()
        ))
                .thenThrow(new CannotAcquireLockException("deadlock"))
                .thenAnswer(invocation -> {
                    IssueAttemptCallback callback = invocation.getArgument(4);
                    callback.onPolicyPassed();
                    return new CouponIssueExecutionResult(issueResult(), false);
                });

        CouponIssueResult actual = coordinator.issue(
                REQUEST_ID, 10L, 20L, MembershipGrade.GOLD, IDEMPOTENCY_KEY);

        assertThat(actual).isEqualTo(issueResult());
        verify(operationExecutionService, times(2)).issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD), eq(IDEMPOTENCY_KEY), any());
    }

    /**
     * <b>운영에서 오는 모양은 감싸인 쪽이다.</b> 저장소 어댑터가 {@code DataAccessException}
     * 을 {@code CouponPersistenceException} 으로 감싸므로, 원본 타입만 잡으면
     * <b>재시도가 사실상 안 돈다.</b> 처음에 그렇게 만들었고 리뷰가 잡았다.
     */
    @Test
    @DisplayName("어댑터가 감싼 락 경합도 다시 시도한다")
    void retriesWhenLockContentionIsWrappedByAnAdapter() {
        prepareContext();
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD), eq(IDEMPOTENCY_KEY), any()
        ))
                .thenThrow(new CouponPersistenceException(
                        "쿠폰 재고 점유에 실패했습니다.",
                        new CannotAcquireLockException("deadlock")))
                .thenAnswer(invocation -> {
                    IssueAttemptCallback callback = invocation.getArgument(4);
                    callback.onPolicyPassed();
                    return new CouponIssueExecutionResult(issueResult(), false);
                });

        CouponIssueResult actual = coordinator.issue(
                REQUEST_ID, 10L, 20L, MembershipGrade.GOLD, IDEMPOTENCY_KEY);

        assertThat(actual).isEqualTo(issueResult());
        verify(operationExecutionService, times(2)).issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD), eq(IDEMPOTENCY_KEY), any());
    }

    /**
     * <b>락 경합이 아닌 실패는 한 번에 끝낸다.</b> 넓게 잡아 아무 실패나 재시도하면 진짜
     * 결함을 세 번 반복하고 응답만 느려진다.
     */
    @Test
    @DisplayName("락 경합이 아닌 영속 실패는 다시 시도하지 않는다")
    void doesNotRetryPersistenceFailuresThatAreNotLockContention() {
        prepareContext();
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD), eq(IDEMPOTENCY_KEY), any()
        )).thenThrow(new CouponPersistenceException(
                "쿠폰 재고 점유에 실패했습니다.", new IllegalStateException("다른 이유")));

        assertThatThrownBy(() -> coordinator.issue(
                REQUEST_ID, 10L, 20L, MembershipGrade.GOLD, IDEMPOTENCY_KEY))
                .isInstanceOf(CouponPersistenceException.class);

        verify(operationExecutionService, times(1)).issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD), eq(IDEMPOTENCY_KEY), any());
    }

    /**
     * <b>상한이나 시간 예산에 닿으면 그대로 던진다.</b> 무한히 다시 시도하면 응답만 느려지고
     * 원인이 안 드러난다. 원인이 반복 데드락인지 지속 병목인지는 코드가 구분하지 못하므로
     * 단정하지 않고, 사실(상한까지 갔다)만 로그와 지표에 남긴다.
     */
    @Test
    @DisplayName("락 경합이 상한까지 이어지면 그대로 실패시킨다")
    void stopsRetryingAfterTheAttemptLimit() {
        prepareContext();
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD), eq(IDEMPOTENCY_KEY), any()
        )).thenThrow(new CannotAcquireLockException("deadlock"));

        assertThatThrownBy(() -> coordinator.issue(
                REQUEST_ID, 10L, 20L, MembershipGrade.GOLD, IDEMPOTENCY_KEY))
                .isInstanceOf(CannotAcquireLockException.class);

        verify(operationExecutionService, times(3)).issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD), eq(IDEMPOTENCY_KEY), any());
    }

    @Test
    void recordsOneAttemptAndOneResultForANewIssue() {
        prepareContext();
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenAnswer(invocation -> {
            IssueAttemptCallback callback = invocation.getArgument(4);
            callback.onPolicyPassed();
            return new CouponIssueExecutionResult(issueResult(), false);
        });

        CouponIssueResult actual = coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isEqualTo(issueResult());
        verify(observationService).recordIssueAttempt(context);
        verify(session).completeIssued(100L, "ABCDEFGHJKLM2345");
        verify(session).finish();
    }

    @Test
    void recordsOnlyAReplayedAttemptForADoneReplay() {
        prepareContext();
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenReturn(new CouponIssueExecutionResult(issueResult(), true));

        CouponIssueResult actual = coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isEqualTo(issueResult());
        ArgumentCaptor<IssuanceFlowEvent.Ctx> contextCaptor =
                ArgumentCaptor.forClass(IssuanceFlowEvent.Ctx.class);
        verify(observationService).recordIssueAttempt(
                contextCaptor.capture()
        );
        assertThat(contextCaptor.getValue()).isEqualTo(
                context.withReplayed(true)
        );
        verify(session, never()).completeIssued(any(Long.class), any());
        verify(session, never()).completeIssueRejected(
                anyInt(),
                any(ReasonCode.class),
                any(Dependency.class)
        );
        verify(session).finish();
    }

    @Test
    void doesNotRecordASecondAttemptWhenAuthoritativeContentionReplays() {
        prepareContext();
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenAnswer(invocation -> {
            IssueAttemptCallback callback = invocation.getArgument(4);
            callback.onPolicyPassed();
            return new CouponIssueExecutionResult(issueResult(), true);
        });

        CouponIssueResult actual = coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isEqualTo(issueResult());
        verify(observationService).recordIssueAttempt(context);
        verify(observationService, never()).recordIssueAttempt(
                context.withReplayed(true)
        );
        verify(session, never()).completeIssued(any(Long.class), any());
        verify(session).finish();
    }

    @ParameterizedTest
    @CsvSource({
            "NOT_OPENED, 409, NOT_OPENED, NONE",
            "COUPON_ROUND_CLOSED, 409, COUPON_ROUND_CLOSED, NONE",
            "GRADE_NOT_ELIGIBLE, 403, GRADE_NOT_ELIGIBLE, NONE"
    })
    void recordsPolicyRejectionWithoutAnAttempt(
            CouponIssueErrorCode errorCode,
            int expectedHttpStatus,
            ReasonCode expectedReasonCode,
            Dependency expectedDependency
    ) {
        prepareContext();
        BusinessException rejected = new BusinessException(
                errorCode
        );
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenThrow(rejected);

        assertThatThrownBy(() -> coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).isSameAs(rejected);

        verify(observationService, never()).recordIssueAttempt(any());
        verify(session).completeIssueRejected(
                expectedHttpStatus,
                expectedReasonCode,
                expectedDependency
        );
        verify(session).finish();
    }

    @ParameterizedTest
    @CsvSource({
            "ALREADY_ISSUED, 409, ALREADY_ISSUED, NONE",
            "SOLD_OUT, 409, STOCK_EXHAUSTED, NONE",
            "COUPON_ISSUE_SAVE_FAILED, 500, INTERNAL_ERROR, MYSQL"
    })
    void keepsAttemptWhenAuthoritativeExecutionRejects(
            CouponIssueErrorCode errorCode,
            int expectedHttpStatus,
            ReasonCode expectedReasonCode,
            Dependency expectedDependency
    ) {
        prepareContext();
        BusinessException rejected = new BusinessException(
                errorCode
        );
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenAnswer(invocation -> {
            IssueAttemptCallback callback = invocation.getArgument(4);
            callback.onPolicyPassed();
            throw rejected;
        });

        assertThatThrownBy(() -> coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).isSameAs(rejected);

        verify(observationService).recordIssueAttempt(context);
        verify(session).completeIssueRejected(
                expectedHttpStatus,
                expectedReasonCode,
                expectedDependency
        );
        verify(session).finish();
    }

    @Test
    void classifiesUnexpectedDatabaseFailureWithoutChangingTheException() {
        prepareContext();
        DataIntegrityViolationException databaseFailure =
                new DataIntegrityViolationException("db unavailable");
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenAnswer(invocation -> {
            IssueAttemptCallback callback = invocation.getArgument(4);
            callback.onPolicyPassed();
            throw databaseFailure;
        });

        assertThatThrownBy(() -> coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).isSameAs(databaseFailure);

        verify(observationService).recordIssueAttempt(context);
        verify(session).completeIssueRejected(
                500,
                ReasonCode.INTERNAL_ERROR,
                Dependency.MYSQL
        );
        verify(session).finish();
    }

    @Test
    void contextFactoryFailureDoesNotChangeTheBusinessResult() {
        when(contextFactory.create(
                REQUEST_ID, 20L, 10L, MembershipGrade.GOLD, EngineVersion.V1
        )).thenThrow(new IllegalStateException("runtime config unavailable"));
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenReturn(new CouponIssueExecutionResult(issueResult(), false));

        CouponIssueResult actual = coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isEqualTo(issueResult());
        verifyNoInteractions(observationService, session);
    }

    @Test
    void attemptRecorderFailureDoesNotChangeTheBusinessResult() {
        prepareContext();
        doThrow(new IllegalStateException("recorder unavailable"))
                .when(observationService).recordIssueAttempt(context);
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenAnswer(invocation -> {
            IssueAttemptCallback callback = invocation.getArgument(4);
            callback.onPolicyPassed();
            return new CouponIssueExecutionResult(issueResult(), false);
        });

        CouponIssueResult actual = coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isEqualTo(issueResult());
        verify(session).completeIssued(100L, "ABCDEFGHJKLM2345");
        verify(session).finish();
    }

    @Test
    void sessionFailureDoesNotChangeTheBusinessResultAndFinishRunsOnce() {
        prepareContext();
        doThrow(new IllegalStateException("completion failed"))
                .when(session).completeIssued(100L, "ABCDEFGHJKLM2345");
        doThrow(new IllegalStateException("finish failed"))
                .when(session).finish();
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenReturn(new CouponIssueExecutionResult(issueResult(), false));

        CouponIssueResult actual = coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isEqualTo(issueResult());
        verify(session).completeIssued(100L, "ABCDEFGHJKLM2345");
        verify(session).finish();
    }

    @Test
    void observationFailuresDoNotReplaceTheAuthoritativeException() {
        prepareContext();
        BusinessException rejected = new BusinessException(
                CouponIssueErrorCode.ALREADY_ISSUED
        );
        doThrow(new IllegalStateException("attempt failed"))
                .when(observationService).recordIssueAttempt(context);
        doThrow(new IllegalStateException("completion failed"))
                .when(session).completeIssueRejected(
                        409,
                        ReasonCode.ALREADY_ISSUED,
                        Dependency.NONE
                );
        doThrow(new IllegalStateException("finish failed"))
                .when(session).finish();
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenAnswer(invocation -> {
            IssueAttemptCallback callback = invocation.getArgument(4);
            callback.onPolicyPassed();
            throw rejected;
        });

        assertThatThrownBy(() -> coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).isSameAs(rejected);

        verify(session).completeIssueRejected(
                409,
                ReasonCode.ALREADY_ISSUED,
                Dependency.NONE
        );
        verify(session).finish();
    }

    @Test
    void recordsExplicitAlreadyIssuedMappingDespiteDatabaseCause() {
        CouponAlreadyIssuedException failure =
                new CouponAlreadyIssuedException(
                        "duplicate issuance",
                        new DataIntegrityViolationException("duplicate key")
                );

        IssuanceFlowEvent resultEvent = recordActualFailureEvent(
                failure,
                true
        );

        assertThat(resultEvent.httpStatus()).isEqualTo(409);
        assertThat(resultEvent.reasonCode()).isEqualTo(
                ReasonCode.ALREADY_ISSUED
        );
        assertThat(resultEvent.dependency()).isEqualTo(Dependency.NONE);
    }

    @Test
    void recordsExplicitMemberNotFoundMappingDespiteDatabaseCause() {
        CouponIssueMemberNotFoundException failure =
                new CouponIssueMemberNotFoundException(
                        "missing member",
                        new DataIntegrityViolationException("foreign key")
                );

        IssuanceFlowEvent resultEvent = recordActualFailureEvent(
                failure,
                true
        );

        assertThat(resultEvent.httpStatus()).isEqualTo(404);
        assertThat(resultEvent.reasonCode()).isEqualTo(ReasonCode.UNMAPPED);
        assertThat(resultEvent.dependency()).isEqualTo(Dependency.NONE);
    }

    @Test
    void infersInternalMysqlForUnmappedPersistenceBusinessFailure() {
        CouponPersistenceException failure = new CouponPersistenceException(
                "persistence unavailable",
                new DataIntegrityViolationException("database unavailable")
        );

        IssuanceFlowEvent resultEvent = recordActualFailureEvent(
                failure,
                false
        );

        assertThat(resultEvent.httpStatus()).isEqualTo(500);
        assertThat(resultEvent.reasonCode()).isEqualTo(
                ReasonCode.INTERNAL_ERROR
        );
        assertThat(resultEvent.dependency()).isEqualTo(Dependency.MYSQL);
    }

    @Test
    void mapperReasonFailurePreservesBusinessExceptionAndFinishesOnce() {
        prepareContext();
        BusinessException failure = new BusinessException(
                CouponIssueErrorCode.ALREADY_ISSUED
        );
        CouponIssueObservationDependencyMapper failingMapper = spy(
                new CouponIssueObservationDependencyMapper()
        );
        doThrow(new IllegalStateException("reason mapping failed"))
                .when(failingMapper).reasonCode(failure);
        CouponIssueObservationCoordinator failingCoordinator =
                coordinator(failingMapper);
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenThrow(failure);

        assertThatThrownBy(() -> failingCoordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).isSameAs(failure);

        verify(failingMapper).reasonCode(failure);
        verify(session, never()).completeIssueRejected(
                anyInt(),
                any(ReasonCode.class),
                any(Dependency.class)
        );
        verify(session).finish();
    }

    @Test
    void mapperDependencyFailurePreservesRuntimeExceptionAndFinishesOnce() {
        prepareContext();
        IllegalStateException failure = new IllegalStateException(
                "business failed"
        );
        CouponIssueObservationDependencyMapper failingMapper = spy(
                new CouponIssueObservationDependencyMapper()
        );
        doThrow(new IllegalStateException("dependency mapping failed"))
                .when(failingMapper).dependency(failure);
        CouponIssueObservationCoordinator failingCoordinator =
                coordinator(failingMapper);
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenThrow(failure);

        assertThatThrownBy(() -> failingCoordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).isSameAs(failure);

        verify(session, never()).completeIssueRejected(
                anyInt(),
                any(ReasonCode.class),
                any(Dependency.class)
        );
        verify(session).finish();
    }

    @Test
    void beginFailurePreservesOriginalBusinessResultWithoutFinishing() {
        when(contextFactory.create(
                REQUEST_ID, 20L, 10L, MembershipGrade.GOLD, EngineVersion.V1
        )).thenReturn(Optional.of(context));
        when(observationService.begin(context)).thenThrow(
                new IllegalStateException("session unavailable")
        );
        CouponIssueResult expected = issueResult();
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenReturn(new CouponIssueExecutionResult(expected, false));

        CouponIssueResult actual = coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isSameAs(expected);
        verifyNoInteractions(session);
    }

    @Test
    void beginFailurePreservesOriginalExceptionWithoutFinishing() {
        when(contextFactory.create(
                REQUEST_ID, 20L, 10L, MembershipGrade.GOLD, EngineVersion.V1
        )).thenReturn(Optional.of(context));
        when(observationService.begin(context)).thenThrow(
                new IllegalStateException("session unavailable")
        );
        BusinessException failure = new BusinessException(
                CouponIssueErrorCode.NOT_OPENED
        );
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenThrow(failure);

        assertThatThrownBy(() -> coordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).isSameAs(failure);

        verifyNoInteractions(session);
    }

    private void prepareContext() {
        when(contextFactory.create(
                REQUEST_ID, 20L, 10L, MembershipGrade.GOLD, EngineVersion.V1
        )).thenReturn(Optional.of(context));
        when(observationService.begin(context)).thenReturn(session);
    }

    /** 프로덕션과 같은 라우터 경유 배선으로 V1 회차를 흘린다. */
    private static CouponIssuanceRouter v1Router() {
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
                                couponRoundId, 30, EngineVersion.V1));
                    }

                    @Override
                    public boolean updateEngineVersionWhenNotOpen(
                            long couponRoundId,
                            EngineVersion engineVersion
                    ) {
                        throw new UnsupportedOperationException();
                    }
                }));
    }

    @SuppressWarnings("unchecked")
    private static ObjectProvider<V2CouponIssueService> noV2Service() {
        return mock(ObjectProvider.class);
    }

    private CouponIssueObservationCoordinator coordinator(
            CouponIssueObservationDependencyMapper mapper
    ) {
        return new CouponIssueObservationCoordinator(
                operationExecutionService,
                contextFactory,
                observationService,
                mapper,
                v1Router(),
                noV2Service(),
                new V2IssuanceOutcomeMeters(new SimpleMeterRegistry()),
                new IssueLockRetryMeters(new SimpleMeterRegistry()),
                IssueLockRetryProperties.defaults(),
                new ObservationIssuanceProperties(null, "api-1", null, null, null),
                new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC))
        );
    }

    private IssuanceFlowEvent recordActualFailureEvent(
            RuntimeException failure,
            boolean policyPassed
    ) {
        List<IssuanceFlowEvent> events = new CopyOnWriteArrayList<>();
        TimeProvider timeProvider = new TimeProvider(Clock.fixed(
                AT,
                ZoneOffset.UTC
        ));
        IssuanceObservationService actualObservationService =
                new IssuanceObservationService(
                        new IssuanceFlowEventFactory(
                                () -> UUID.fromString(
                                        "11111111-1111-1111-1111-111111111111"
                                )
                        ),
                        events::add,
                        timeProvider
                );
        when(contextFactory.create(
                REQUEST_ID, 20L, 10L, MembershipGrade.GOLD, EngineVersion.V1
        )).thenReturn(Optional.of(context));
        when(operationExecutionService.issueWithMetadata(
                eq(10L), eq(20L), eq(MembershipGrade.GOLD),
                eq(IDEMPOTENCY_KEY), any()
        )).thenAnswer(invocation -> {
            if (policyPassed) {
                IssueAttemptCallback callback = invocation.getArgument(4);
                callback.onPolicyPassed();
            }
            throw failure;
        });
        CouponIssueObservationCoordinator actualCoordinator =
                new CouponIssueObservationCoordinator(
                        operationExecutionService,
                        contextFactory,
                        actualObservationService,
                        new CouponIssueObservationDependencyMapper(),
                        v1Router(),
                        noV2Service(),
                        new V2IssuanceOutcomeMeters(new SimpleMeterRegistry()),
                new IssueLockRetryMeters(new SimpleMeterRegistry()),
                IssueLockRetryProperties.defaults(),
                        new ObservationIssuanceProperties(null, "api-1", null, null, null),
                        timeProvider
                );

        assertThatThrownBy(() -> actualCoordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).isSameAs(failure);

        return events.stream()
                .filter(event -> event.eventType() == EventType.ISSUE_RESULT)
                .findFirst()
                .orElseThrow();
    }

    private CouponIssueResult issueResult() {
        return new CouponIssueResult(
                100L,
                10L,
                "ABCDEFGHJKLM2345",
                IssuanceStatus.ISSUED,
                AT,
                AT.plusSeconds(604_800)
        );
    }
}
