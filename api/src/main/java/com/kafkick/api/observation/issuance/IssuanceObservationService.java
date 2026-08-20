package com.kafkick.api.observation.issuance;

import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.support.TimeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 발급 요청별 관측 결과를 안전하게 기록할 수 있는 Session을 생성합니다.
 *
 * <p>서비스는 이벤트 생성기와 기록 포트를 Session에 전달할 뿐 쿠폰 발급 도메인이나 HTTP 계층을
 * 직접 참조하지 않습니다. 따라서 발급 기능의 구현 상태와 무관하게 공통 기록 규칙을 유지할 수 있습니다.
 *
 * <p>쿠폰 발급 HTTP 흐름이 구현되면 실제 요청 경계에서 {@link #begin(IssuanceFlowEvent.Ctx)}을
 * 호출해 요청 한 건에 대응하는 Session을 생성합니다.
 */
public final class IssuanceObservationService {

    private static final Logger log = LoggerFactory.getLogger(IssuanceObservationService.class);

    private final IssuanceFlowEventFactory eventFactory;
    private final EventRecorder eventRecorder;
    private final TimeProvider timeProvider;

    public IssuanceObservationService(
            IssuanceFlowEventFactory eventFactory,
            EventRecorder eventRecorder,
            TimeProvider timeProvider
    ) {
        this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory");
        this.eventRecorder = Objects.requireNonNull(eventRecorder, "eventRecorder");
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
    }

    /**
     * 발급 요청 한 건의 관측 수명주기를 관리하는 Session을 생성합니다.
     *
     * <p>호출자는 업무 결과가 확정되면 Session의 완료 메서드 중 하나를 호출한 뒤
     * {@link IssuanceObservationSession#finish()}로 기록을 종료해야 합니다.
     *
     * <p>실제 회원·쿠폰·등급 및 런타임 설정을 이용한 Context 조립은 쿠폰 발급 요청 경계가
     * 구현된 뒤 이 메서드 호출부에 연결합니다. Context에 들어온 시각은 결과 이벤트 생성 시
     * 실제 완료 시각으로 교체됩니다.
     *
     * @param context 요청 시작 시점에 확정된 공통 관측 정보
     * @return 요청 한 건에서만 사용하는 관측 Session
     * @throws NullPointerException context가 {@code null}인 경우
     */
    public IssuanceObservationSession begin(IssuanceFlowEvent.Ctx context) {
        return new IssuanceObservationSession(
                Objects.requireNonNull(context, "context"),
                eventFactory,
                eventRecorder,
                timeProvider
        );
    }

    /**
     * 대기열에서 최초 입장이 허용된 사실을 독립된 이벤트로 안전하게 기록합니다.
     *
     * <p>입장 허용은 HTTP 요청 결과 Session과 별개의 사건이므로 Session의 단일 결과와 경쟁시키지
     * 않습니다. 실제 Entry-Token 생성 흐름이 구현되면 토큰이 최초 생성됐다고 확인된 경우에만 이
     * 메서드를 호출합니다.
     *
     * <p>이벤트 생성 또는 기록 포트에서 {@link RuntimeException}이 발생해도 업무 흐름에는 전파하지
     * 않습니다. 실제 기록 구현은 제한된 로컬 큐에 빠르게 인계하고 원격 완료를 기다리지 않아야 합니다.
     *
     * @param context 입장 허용 대상과 실행 설정을 담은 공통 관측 정보
     * @param queueSequence 최초 입장 허용에 대응하는 안정적인 대기열 순번
     * @throws NullPointerException context가 {@code null}인 경우
     */
    public void recordAdmitted(IssuanceFlowEvent.Ctx context, long queueSequence) {
        IssuanceFlowEvent.Ctx requiredContext = Objects.requireNonNull(context, "context");
        try {
            IssuanceFlowEvent.Ctx admittedContext = requiredContext
                    .withOccurredAt(timeProvider.instant());
            IssuanceFlowEvent event = eventFactory.admitted(admittedContext, queueSequence);
            eventRecorder.record(event);
        } catch (RuntimeException exception) {
            log.debug("대기열 입장 관측 이벤트 기록에 실패했습니다. 업무 흐름은 계속 진행합니다.", exception);
        }
    }
}
