package com.kafkick.api.observation.issuance;

import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.support.TimeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

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

    private static final long LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final IssuanceFlowEventFactory eventFactory;
    private final EventRecorder eventRecorder;
    private final TimeProvider timeProvider;
    private final LongSupplier timeSource;
    private final AtomicLong attemptFailures = new AtomicLong();
    private final AtomicLong nextFailureLogAtNanos;

    public IssuanceObservationService(
            IssuanceFlowEventFactory eventFactory,
            EventRecorder eventRecorder,
            TimeProvider timeProvider
    ) {
        this(eventFactory, eventRecorder, timeProvider, System::nanoTime);
    }

    /** 유량 제한 간격을 검증할 수 있게 단조 시계를 주입받는 생성자입니다. */
    IssuanceObservationService(
            IssuanceFlowEventFactory eventFactory,
            EventRecorder eventRecorder,
            TimeProvider timeProvider,
            LongSupplier timeSource
    ) {
        this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory");
        this.eventRecorder = Objects.requireNonNull(eventRecorder, "eventRecorder");
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
        this.nextFailureLogAtNanos = new AtomicLong(timeSource.getAsLong());
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
     * 정책 검증을 통과한 요청이 발급 엔진에 진입한 사실을 독립된 이벤트로 기록합니다.
     *
     * <p>Session이 아니라 서비스에 두는 이유는 Session의 결과가 요청당 한 건만 채택되기 때문입니다.
     * 시도를 결과로 등록하면 뒤따르는 실제 발급 결과가 버려집니다.
     *
     * <p>실제 기록 구현은 제한된 로컬 큐에 빠르게 인계하고 원격 완료를 기다리지 않아야 합니다
     * ({@link #recordAdmitted}와 같은 요구입니다). <b>현재 구현은 이를 지키지 않습니다</b> —
     * Kafka 발행이라 브로커 메타데이터가 없거나 버퍼가 차면 {@code max.block.ms}(50ms)만큼 호출
     * 스레드를 붙잡습니다. 측정 기준점이 클라이언트 응답 시점이라 그 시간은 공식 지연에 포함됩니다.
     * TODO(미발급): 인계 경계를 두거나 이 요구를 실제 거동에 맞춰 낮춘다.
     *
     * <p>호출 위치는 정책 검증(오픈·등급·대기열/Entry Token·중복)을 통과한 뒤, 재고 차감과 발급
     * 저장을 시작하기 <b>직전</b>입니다 — 재고 락과 발급 트랜잭션을 잡기 <b>전</b>이어야 합니다.
     * 위 대기가 락 안에서 일어나면 그대로 락 보유 시간이 되어 선착순 경합 전체가 밀립니다.
     *
     * <p>이벤트 생성 또는 기록 포트에서 {@link RuntimeException}이 발생해도 업무 흐름에는
     * 전파하지 않습니다.
     *
     * <p>Context에 담겨 온 {@code occurredAt}은 <b>이 메서드 호출 시각으로 교체됩니다</b>. 시도는
     * 요청 시작이 아니라 발급 엔진 진입 시점의 사건이므로, 요청 시작 시 만든 Context를 그대로
     * 넘겨도 시작 시각이 시도 시각으로 기록되지 않습니다. {@link #recordAdmitted}와 같은 규칙입니다.
     *
     * @param context 시도 대상과 실행 설정을 담은 공통 관측 정보
     * @throws NullPointerException context가 {@code null}인 경우
     */
    public void recordIssueAttempt(IssuanceFlowEvent.Ctx context) {
        IssuanceFlowEvent.Ctx requiredContext = Objects.requireNonNull(context, "context");
        try {
            IssuanceFlowEvent.Ctx attemptContext = requiredContext
                    .withOccurredAt(timeProvider.instant());
            IssuanceFlowEvent event = eventFactory.issueAttempt(attemptContext);
            eventRecorder.record(event);
        } catch (RuntimeException exception) {
            logFailureAtMostOncePerInterval(requiredContext, exception);
        }
    }

    /**
     * attempt 기록 실패를 최대 10초에 한 번만 남깁니다.
     *
     * <p>레벨을 {@code warn}으로 둔 이유는 attempt 가 무트래픽과 발급 중단을 가르는 값이라
     * 전량 소실이 화면에서 0과 구분되지 않기 때문입니다({@code recordAdmitted}가 {@code debug}인
     * 것과 갈리는 지점입니다). 그런데 이벤트 생성 실패는 계약 위반이라 요청마다 결정론적으로
     * 재발합니다 — 스파이크 구간에서 건당 WARN 을 남기면 logback 동기 appender 가 발급 요청
     * 스레드를 붙잡아 측정 자체를 오염시킵니다. {@code AttemptFailureCounter}가 같은 이유로
     * 같은 간격을 씁니다.
     *
     * <p>스택트레이스를 넘기지 않습니다. 포맷 비용을 요청 스레드가 뭅니다.
     * 누적 건수를 함께 실어 조인 만큼이 침묵으로 남지 않게 합니다.
     */
    private void logFailureAtMostOncePerInterval(
            IssuanceFlowEvent.Ctx context,
            RuntimeException exception
    ) {
        long total = attemptFailures.incrementAndGet();
        long now = timeSource.getAsLong();
        long due = nextFailureLogAtNanos.get();
        // 실패가 몰릴 때 이 자리에 락을 두면 그 락이 발급 경로의 병목이 된다.
        if (now - due < 0 || !nextFailureLogAtNanos.compareAndSet(due, now + LOG_INTERVAL_NANOS)) {
            return;
        }
        log.warn(
                "발급 시도 관측 이벤트 기록에 실패했습니다. 업무 흐름은 계속 진행합니다. "
                        + "누적 {}건, requestId={}, eventType={}, cause={}",
                total,
                context.requestId(),
                EventType.ISSUE_ATTEMPT,
                exception.toString()
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
