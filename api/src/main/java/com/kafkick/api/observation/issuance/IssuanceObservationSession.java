package com.kafkick.api.observation.issuance;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 발급 요청 한 건에서 확정된 관측 결과를 최대 한 번 기록하는 요청 단위 객체입니다.
 *
 * <p>완료 결과와 종료 상태는 {@link AtomicReference} 하나로 관리합니다. 성공과 실패가 경합하면
 * 먼저 등록된 결과만 채택하며, 여러 호출자가 동시에 종료하더라도 한 호출자만 이벤트를 기록합니다.
 * 이 보장은 중복 관측 이벤트를 막기 위한 것이며 쿠폰의 중복 발급을 제어하지는 않습니다.
 *
 * <p>각 완료 결과는 완료 메서드가 호출된 시각을 함께 보관합니다. 따라서 이벤트 발생 시각은
 * {@code finish()} 또는 Recorder 호출 시각이 아니라 최초 채택된 업무 결과의 완료 시각입니다.
 *
 * <p>{@code finish()}가 완료 결과보다 먼저 호출되면 결과 없이 종료되고 이후 완료 호출은 무시됩니다.
 * 정상적인 호출 순서는 완료 메서드 호출 후 {@link #finish()}입니다.
 */
public final class IssuanceObservationSession {

    private static final Logger log = LoggerFactory.getLogger(IssuanceObservationSession.class);
    private static final FinishedOutcome FINISHED = new FinishedOutcome();

    private final IssuanceFlowEvent.Ctx context;
    private final IssuanceFlowEventFactory eventFactory;
    private final EventRecorder eventRecorder;
    private final TimeProvider timeProvider;
    private final AtomicReference<SessionState> state = new AtomicReference<>();

    IssuanceObservationSession(
            IssuanceFlowEvent.Ctx context,
            IssuanceFlowEventFactory eventFactory,
            EventRecorder eventRecorder,
            TimeProvider timeProvider
    ) {
        this.context = Objects.requireNonNull(context, "context");
        this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory");
        this.eventRecorder = Objects.requireNonNull(eventRecorder, "eventRecorder");
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
    }

    /**
     * 발급 성공 결과를 Session의 첫 완료 결과로 등록합니다.
     *
     * <p>이미 다른 결과가 등록됐거나 Session이 종료됐다면 이 호출은 상태를 변경하지 않습니다.
     * 값의 세부 계약은 이벤트 생성 시 공통 {@link IssuanceFlowEventFactory}가 검증합니다.
     *
     * @param issuanceId 생성된 발급 식별자
     * @param issuanceCode 사용자에게 발급된 쿠폰 코드
     */
    public void completeIssued(long issuanceId, String issuanceCode) {
        complete(new IssuedOutcome(issuanceId, issuanceCode, timeProvider.instant()));
    }

    /**
     * 발급 거절 또는 실패 결과를 Session의 첫 완료 결과로 등록합니다.
     *
     * <p>오류 코드가 관측용 사유를 제공하지 않으면 {@link ReasonCode#UNMAPPED}로 기록합니다.
     * 쿠폰 오류별 사유 매핑이 구현되면 각 {@link ErrorCode#reasonCode()} 결과가 이 경계를 통해
     * 이벤트에 연결됩니다.
     *
     * <p>HTTP 상태는 별도로 전달받지 않고 {@link ErrorCode#getStatus()}에서 파생해 실제 응답과
     * 관측 이벤트가 서로 다른 상태를 기록할 수 없게 합니다.
     *
     * @param errorCode 실패 상태와 사유를 제공하는 업무 오류 코드
     * @param dependency 실패와 직접 관련된 외부 의존성
     */
    public void completeIssueRejected(ErrorCode errorCode, Dependency dependency) {
        complete(new IssueRejectedOutcome(errorCode, dependency, timeProvider.instant()));
    }

    /**
     * 대기 없이 즉시 입장이 허용된 결과를 Session의 첫 완료 결과로 등록합니다.
     *
     * <p>이 결과는 HTTP 200의 {@code ENTRY_RESULT}로 기록되며 대기 위치와 순번을 포함하지 않습니다.
     * 대기열에서 기다리던 사용자의 입장 허용을 나타내는 {@code QUEUE_ADMITTED} 이벤트와는 다른
     * 요청 결과입니다. 실제 진입 흐름이 구현되면 즉시 입장을 반환하는 경계에서 호출합니다.
     */
    public void completeEntryAdmitted() {
        complete(new EntryAdmittedOutcome(timeProvider.instant()));
    }

    /**
     * 대기열에 등록된 결과를 Session의 첫 완료 결과로 등록합니다.
     *
     * <p>이 결과는 HTTP 202의 {@code ENTRY_RESULT}로 기록됩니다. 호출자가 임의의 성공 상태를
     * 전달하지 못하게 상태를 메서드 내부에서 고정하며, 대기 위치와 순번의 값 계약은 공통 이벤트
     * Factory가 검증합니다.
     *
     * @param queuePosition 대기열에서 사용자에게 표시할 위치
     * @param queueSequence 대기열의 안정적인 진입 순번
     */
    public void completeEntryQueued(long queuePosition, long queueSequence) {
        complete(new EntryQueuedOutcome(
                queuePosition,
                queueSequence,
                timeProvider.instant()
        ));
    }

    /**
     * 발급 절차 진입의 거절 또는 실패 결과를 Session의 첫 완료 결과로 등록합니다.
     *
     * <p>HTTP 상태는 {@link ErrorCode#getStatus()}에서 파생하며 관측 사유 매핑이 없으면
     * {@link ReasonCode#UNMAPPED}를 사용합니다.
     *
     * @param errorCode 실패 상태와 사유를 제공하는 업무 오류 코드
     * @param dependency 실패와 직접 관련된 외부 의존성
     */
    public void completeEntryRejected(ErrorCode errorCode, Dependency dependency) {
        complete(new EntryRejectedOutcome(errorCode, dependency, timeProvider.instant()));
    }

    /**
     * 등록된 결과를 이벤트로 생성해 기록하고 Session을 종료합니다.
     *
     * <p>완료 결과가 없거나 이미 종료된 Session이면 아무 이벤트도 기록하지 않습니다. 이벤트 필드
     * 검증 또는 기록 포트에서 {@link RuntimeException}이 발생하면 로그만 남기고 예외를 호출자에게
     * 전파하지 않습니다. 관측 시스템의 실패가 쿠폰 발급 결과를 바꾸지 않도록 하기 위함입니다.
     */
    public void finish() {
        // 결과 조회와 FINISHED 전환을 원자적으로 수행해 한 호출만 기록 책임을 갖게 합니다.
        SessionState previous = state.getAndSet(FINISHED);
        if (!(previous instanceof Outcome completed)) {
            return;
        }

        try {
            IssuanceFlowEvent.Ctx resultContext = context.withOccurredAt(completed.completedAt());
            IssuanceFlowEvent event = completed.createEvent(eventFactory, resultContext);
            eventRecorder.record(event);
        } catch (RuntimeException exception) {
            log.debug(
                    "발급 관측 이벤트 기록에 실패했습니다. 업무 흐름은 계속 진행합니다. requestId={}",
                    context.requestId(),
                    exception
            );
        }
    }

    /**
     * 아직 완료 결과가 없는 Session에 첫 번째 결과를 등록합니다.
     *
     * <p>성공과 실패가 서로 다른 실행 흐름에서 동시에 도착할 수 있으므로 단순 대입 대신
     * 원자적 비교 후 교체를 사용합니다. 이미 결과가 등록됐거나 {@link #finish()}가 먼저 실행돼
     * 종료 상태가 됐다면 새 결과는 기존 상태를 덮어쓰지 않습니다.
     *
     * @param completed 새로 등록을 시도할 발급 관측 결과
     */
    private void complete(Outcome completed) {
        // 결과와 그 결과의 완료 시각을 함께 CAS해 최초로 채택된 한 쌍을 그대로 보존합니다.
        state.compareAndSet(null, completed);
    }

    /**
     * 업무 오류 코드가 제공하는 관측용 실패 사유를 결정합니다.
     *
     * <p>오류 코드가 아직 관측 사유와 매핑되지 않은 경우에도 실패 이벤트 자체는 보존해야 하므로
     * {@link ReasonCode#UNMAPPED}를 반환합니다.
     *
     * @param errorCode 업무 흐름에서 발생한 오류 코드
     * @return 오류 코드에 매핑된 사유 또는 {@link ReasonCode#UNMAPPED}
     */
    private static ReasonCode resolveReasonCode(ErrorCode errorCode) {
        if (errorCode == null) {
            return ReasonCode.UNMAPPED;
        }
        return errorCode.reasonCode().orElse(ReasonCode.UNMAPPED);
    }

    /** Session의 원자적 상태 참조에 저장할 수 있는 완료 결과와 종료 표식의 공통 타입입니다. */
    private sealed interface SessionState permits Outcome, FinishedOutcome {
    }

    /**
     * Session이 보관하는 확정된 업무 결과의 공통 계약입니다.
     *
     * <p>각 결과는 최초 완료 시각과 자신에게 필요한 업무 값만 보관하고, 기록 시점에 공통 Factory를
     * 사용해 {@link IssuanceFlowEvent}로 변환합니다. 종료 표식은 이 계약에서 분리해 업무 결과로
     * 잘못 변환되는 경로를 타입 수준에서 차단합니다.
     */
    private sealed interface Outcome extends SessionState permits
            IssuedOutcome,
            IssueRejectedOutcome,
            EntryAdmittedOutcome,
            EntryQueuedOutcome,
            EntryRejectedOutcome {

        /**
         * 최초 완료 결과가 Session에 등록을 시도한 시각을 반환합니다.
         *
         * @return {@code finish()} 시각과 독립적인 실제 결과 완료 시각
         */
        Instant completedAt();

        /**
         * 보관된 결과를 공통 계약에 맞는 발급 관측 이벤트로 변환합니다.
         *
         * @param eventFactory 이벤트 식별자 생성과 필드 계약 검증을 담당하는 Factory
         * @param context 결과 확정 시각을 반영한 공통 관측 정보
         * @return 기록 포트에 전달할 완성된 발급 관측 이벤트
         */
        IssuanceFlowEvent createEvent(
                IssuanceFlowEventFactory eventFactory,
                IssuanceFlowEvent.Ctx context
        );
    }

    /**
     * 발급에 성공해 생성된 발급 식별자와 쿠폰 코드를 보관하는 완료 결과입니다.
     *
     * @param issuanceId 생성된 발급 식별자
     * @param issuanceCode 사용자에게 발급된 쿠폰 코드
     * @param completedAt 발급 성공 결과가 완료된 시각
     */
    private record IssuedOutcome(
            long issuanceId,
            String issuanceCode,
            Instant completedAt
    ) implements Outcome {

        /** 발급 성공 결과를 HTTP 201의 ISSUE_RESULT 이벤트로 변환합니다. */
        @Override
        public IssuanceFlowEvent createEvent(
                IssuanceFlowEventFactory eventFactory,
                IssuanceFlowEvent.Ctx context
        ) {
            return eventFactory.issued(context, issuanceId, issuanceCode);
        }
    }

    /**
     * 발급이 거절되거나 실패했을 때 응답과 장애 정보를 보관하는 완료 결과입니다.
     *
     * @param errorCode 실패를 설명하는 업무 오류 코드
     * @param dependency 실패와 직접 관련된 외부 의존성
     * @param completedAt 발급 실패 결과가 완료된 시각
     */
    private record IssueRejectedOutcome(
            ErrorCode errorCode,
            Dependency dependency,
            Instant completedAt
    ) implements Outcome {

        /**
         * 업무 오류를 관측용 사유로 변환해 실패 ISSUE_RESULT 이벤트를 생성합니다.
         *
         * <p>업무 오류에 명시적인 관측 사유가 없으면 {@link ReasonCode#UNMAPPED}를 사용합니다.
         */
        @Override
        public IssuanceFlowEvent createEvent(
                IssuanceFlowEventFactory eventFactory,
                IssuanceFlowEvent.Ctx context
        ) {
            return eventFactory.issueRejected(
                    context,
                    Objects.requireNonNull(errorCode, "errorCode").getStatus(),
                    resolveReasonCode(errorCode),
                    dependency
            );
        }
    }

    /**
     * 대기 없이 즉시 입장이 허용된 결과를 보관합니다.
     *
     * @param completedAt 즉시 입장 결과가 완료된 시각
     */
    private record EntryAdmittedOutcome(Instant completedAt) implements Outcome {

        /** 즉시 입장 결과를 HTTP 200의 ENTRY_RESULT 이벤트로 변환합니다. */
        @Override
        public IssuanceFlowEvent createEvent(
                IssuanceFlowEventFactory eventFactory,
                IssuanceFlowEvent.Ctx context
        ) {
            return eventFactory.entry(
                    context,
                    200,
                    null,
                    Dependency.NONE,
                    null,
                    null
            );
        }
    }

    /**
     * 대기열에 등록된 결과와 대기 정보를 보관합니다.
     *
     * @param queuePosition 대기열에서 사용자에게 표시할 위치
     * @param queueSequence 대기열의 안정적인 진입 순번
     * @param completedAt 대기열 등록 결과가 완료된 시각
     */
    private record EntryQueuedOutcome(
            long queuePosition,
            long queueSequence,
            Instant completedAt
    ) implements Outcome {

        /** 대기열 등록 결과를 HTTP 202의 ENTRY_RESULT 이벤트로 변환합니다. */
        @Override
        public IssuanceFlowEvent createEvent(
                IssuanceFlowEventFactory eventFactory,
                IssuanceFlowEvent.Ctx context
        ) {
            return eventFactory.entry(
                    context,
                    202,
                    null,
                    Dependency.NONE,
                    queuePosition,
                    queueSequence
            );
        }
    }

    /**
     * 발급 절차 진입이 거절되거나 실패했을 때 오류와 의존성 정보를 보관합니다.
     *
     * @param errorCode 실패 상태와 사유를 제공하는 업무 오류 코드
     * @param dependency 실패와 직접 관련된 외부 의존성
     * @param completedAt 진입 실패 결과가 완료된 시각
     */
    private record EntryRejectedOutcome(
            ErrorCode errorCode,
            Dependency dependency,
            Instant completedAt
    ) implements Outcome {

        /** ErrorCode의 상태와 관측 사유를 사용해 실패 ENTRY_RESULT 이벤트를 생성합니다. */
        @Override
        public IssuanceFlowEvent createEvent(
                IssuanceFlowEventFactory eventFactory,
                IssuanceFlowEvent.Ctx context
        ) {
            return eventFactory.entry(
                    context,
                    Objects.requireNonNull(errorCode, "errorCode").getStatus(),
                    resolveReasonCode(errorCode),
                    dependency,
                    null,
                    null
            );
        }
    }

    /**
     * Session의 기록 책임이 이미 소비됐음을 나타내는 종료 표식입니다.
     *
     * <p>{@link AtomicReference#getAndSet(Object)} 결과가 이 인스턴스라면 다른 호출자가 이미
     * Session을 종료한 것이므로 이벤트 생성과 기록을 수행하지 않습니다.
     */
    private static final class FinishedOutcome implements SessionState {
    }
}
