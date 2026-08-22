package com.kafkick.infra.mq.config;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import com.kafkick.core.observation.SourceStatus;

/**
 * 토픽을 <b>기동을 마친 뒤</b> 만든다. 기동 경로에서 브로커를 기다리지 않는 것이 목적이다.
 *
 * <h2>왜 기동 경로에서 뺐나</h2>
 *
 * {@code KafkaAdmin} 의 기본 동작은 컨텍스트 refresh 중에 동기로 토픽을 만드는 것이다.
 * 브로커가 없으면 그 자리에서 <b>컨텍스트당 약 15초</b>가 걸렸다(실측 — 테스트 4개에 60초,
 * 결과 XML 13.6MB). {@code operationTimeout} 은 작업 하나의 상한이지 전체 상한이 아니다.
 * 롤링 배포 중이라면 인스턴스마다 그만큼 용량이 빈다.
 *
 * <h2>포기하지 않는다</h2>
 *
 * 프로비저닝이 실패한 채로 발급이 시작되면 <b>브로커가 토픽을 대신 만든다</b>
 * ({@code auto.create.topics.enable} 기본값 true, RF 1 · 파티션 1). 그러면 RF3 · ISR2 ·
 * 파티션 6 계약이 통째로 무효가 되고, <b>RF 는 나중에 되돌릴 수 없다</b> — 노드 하나가
 * 빠지면 그 파티션이 사라지고 "영구 미영속 발급" 이 된다.
 *
 * <p>그래서 <b>확인될 때까지 물러서며 계속 시도한다</b>(간격 상한 5분). 예전에는 5회 45초로
 * 끊었는데, 브로커 3대를 롤링 재기동하는 창이 그보다 길면 그 인스턴스는 재기동 전까지 토픽 없이
 * 트래픽을 받았다. 스케줄러를 새로 들이지 않고 <b>이미 도는 스레드가 물러서며 기다리는</b>
 * 방식이라, 컨텍스트가 닫히면 인터럽트로 즉시 빠져나온다.
 *
 * <p>선언과 다른 경우(MISMATCHED)만 예외다 — 재시도가 고치지 못하므로 즉시 끝낸다.
 *
 * <p>그래도 브로커 쪽 {@code auto.create.topics.enable=false} 가 근본 방어다. 이 값을 못 끄는
 * 환경이면 프로비저닝 성공을 배포 전제 조건으로 봐야 한다.
 *
 * <h2>성공 여부를 값으로 남긴다</h2>
 *
 * 실패를 WARN 한 줄로만 남기면 "조용히 계약 위반" 상태를 아무도 모른다. 그래서 상태를
 * {@link SourceStatus} 로 들고 {@link KafkaTopicConfig} 가 값·상태 미터 한 쌍으로 내보낸다.
 *
 * <p><b>0/1 로 내지 않는 이유</b> — 이 저장소는 "값이 없을 때 0 을 실으면 '정상인데 0' 과
 * 구분되지 않는다" 를 규칙으로 못박았다. 0 하나로는 <em>아직 확인 전</em>과 <em>재시도를
 * 소진했다</em>가 같은 값이 되어, 그 위에 경보를 걸면 롤링 배포마다 오탐이 난다.
 */
public class KafkaTopicProvisioner implements ApplicationRunner {

    /** 백오프 상한. 여기까지 벌어진 뒤에는 이 간격으로 계속 확인한다. */
    private static final Duration MAX_BACKOFF = Duration.ofMinutes(5);

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicProvisioner.class);

    private final Supplier<ProvisionOutcome> attemptProvisioning;
    private final ExecutorService executor;
    private final int alertAfterAttempts;
    private final Duration initialBackoff;
    /**
     * 상태와 원인을 <b>하나로</b> 든다. 두 미터가 각각 읽으므로 따로 갱신하면 한 scrape 안에
     * 존재하지 않는 조합({@code PENDING} + {@code mismatched})이 잡힌다.
     */
    public record Snapshot(SourceStatus status, String cause) {
    }

    private final AtomicReference<Snapshot> state =
            new AtomicReference<>(new Snapshot(SourceStatus.PENDING, "none"));

    /**
     * 마지막 실패 원인. 없으면 5회를 소진했을 때 로그에 <b>횟수만</b> 남는다 — 권한(ACL) 문제나
     * 잘못된 호스트명처럼 재기동으로 낫지 않는 실패도 "브로커가 뜬 뒤 재기동하라" 로 안내된다.
     */
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();

    /**
     * 마지막 실패의 <b>종류</b>. 상태 미터의 꼬리표로 나간다 — 지표만 보고
     * "재기동하면 낫는 실패" 와 "재기동해도 안 낫는 실패" 를 갈라야 하기 때문이다.
     * 이 스택에는 로그를 모아 보는 도구가 없어서 로그로 가르라는 지침이 화면에서는 닿지 않는다.
     */


    /**
     * {@code KafkaAdmin} 이 아니라 <b>"한 번 시도하고 결과를 돌려주는 것"</b> 을 받는다.
     * {@code KafkaAdmin.initialize()} 가 {@code final} 이라 스텁할 수 없어서이기도 하지만,
     * 이 클래스가 실제로 필요로 하는 계약이 그것뿐이기도 하다.
     *
     * <p>결과가 세 값인 이유 — <b>실패의 종류에 따라 재시도의 의미가 다르다.</b> 확인하지 못한
     * 것은 다시 해 볼 가치가 있지만, 선언과 다른 것은 재시도가 고치지 못한다.
     */
    public KafkaTopicProvisioner(
            Supplier<ProvisionOutcome> attemptProvisioning,
            ExecutorService executor,
            int alertAfterAttempts,
            Duration initialBackoff
    ) {
        this.attemptProvisioning = attemptProvisioning;
        this.executor = executor;
        this.alertAfterAttempts = alertAfterAttempts;
        this.initialBackoff = initialBackoff;
    }

    /**
     * 기동 스레드는 여기서 즉시 돌아간다. 실제 작업은 컨텍스트가 소유한 실행기에 넘긴다 —
     * 스레드를 직접 띄우면 컨텍스트가 닫힐 때 정리할 대상이 없어, 종료 중 실패가 "브로커
     * 문제" 로 잘못 기록된다.
     */
    @Override
    public void run(ApplicationArguments args) {
        executor.submit(this::provisionOnce);
    }

    /**
     * 예외를 밖으로 내지 않는다. 토픽 생성 실패가 애플리케이션 상태를 바꾸면 안 된다.
     *
     * <p>다만 {@code Error} 는 다르다. {@code submit} 은 그것을 Future 에 가둬 로그조차 안
     * 남기는데, 그러면 상태가 {@code PENDING}("곧 나아진다")에 영구히 머물러 아무도 경보를
     * 못 건다. 그 사이 브로커가 RF1 토픽을 대신 만들면 되돌릴 수 없다.
     */
    void provisionOnce() {
        try {
            provisionLoop();
        } catch (Throwable fatal) {
            state.set(new Snapshot(SourceStatus.UNAVAILABLE, "unconfirmed"));
            log.error("토픽 프로비저닝 태스크가 죽었다 — 이 인스턴스는 더 이상 확인하지 않는다", fatal);
            throw fatal;
        }
    }

    private void provisionLoop() {
        Duration backoff = initialBackoff;
        for (int attempt = 1; ; attempt++) {
            ProvisionOutcome outcome = tryProvision(attempt);
            if (outcome == ProvisionOutcome.PROVISIONED) {
                return;
            }
            if (outcome == ProvisionOutcome.MISMATCHED) {
                // 재시도가 고치지 못한다. 45초를 태우지 않고 끝낸다.
                state.set(new Snapshot(SourceStatus.UNAVAILABLE, causeOf(outcome)));
                log.error("토픽이 선언과 다르다. 재기동으로는 낫지 않는다 — 복제본 수는 파티션"
                        + " 재배치로만 바뀐다. 위 ERROR 가 가리키는 토픽을 다시 만들거나 재배치할 것");
                return;
            }
            if (attempt == alertAfterAttempts) {
                // 포기하지는 않는다. 다만 이쯤부터는 사람이 알아야 한다.
                state.set(new Snapshot(SourceStatus.UNAVAILABLE, causeOf(outcome)));
                log.warn("토픽을 {}회 시도했지만 확인하지 못했다. 계속 재시도한다 — 브로커의 자동"
                        + " 생성이 켜져 있으면 그동안 RF1 토픽이 대신 만들어질 수 있다. 아래 원인이"
                        + " 권한·호스트명처럼 재기동으로 낫지 않는 것인지 먼저 볼 것",
                        alertAfterAttempts, lastFailure.get());
            }
            if (!sleep(backoff)) {
                // 종료 중이다. PENDING 으로 두면 "아직 확인 전" 이라 경보가 안 걸린다 —
                // 실제로는 확인을 포기한 것이다. 이미 VALID 면 덮지 않는다.
                // compareAndSet 은 참조 비교다 — 기대값을 새로 만들면 절대 일치하지 않는다.
                Snapshot current = state.get();
                if (current.status() == SourceStatus.PENDING) {
                    state.compareAndSet(current, new Snapshot(SourceStatus.UNAVAILABLE, "shutdown"));
                }
                return;
            }
            backoff = backoff.multipliedBy(2);
            if (backoff.compareTo(MAX_BACKOFF) > 0) {
                backoff = MAX_BACKOFF;
            }
        }
    }

    public boolean isProvisioned() {
        return state.get().status() == SourceStatus.VALID;
    }

    /** 확인되지 않은 동안은 값이 <b>없다</b>. 0 을 실으면 "확인했는데 0" 과 구분되지 않는다. */
    public double provisionedValue() {
        return isProvisioned() ? 1 : Double.NaN;
    }

    /** 값이 없는 이유. 값 미터와 짝으로 나간다. */
    public SourceStatus status() {
        return state.get().status();
    }

    private static String causeOf(ProvisionOutcome outcome) {
        return outcome == ProvisionOutcome.MISMATCHED ? "mismatched" : "unconfirmed";
    }

    /** 상태와 원인을 <b>같이</b> 읽는다. 미터 둘이 각각 읽으면 어긋난 조합이 나온다. */
    public Snapshot snapshot() {
        return state.get();
    }

    /**
     * 마지막 실패의 종류. 상태 미터의 꼬리표 값이다 — <b>닫힌 3값</b>이라 시계열이 늘지 않는다.
     *
     * <p>{@code unconfirmed} 는 재기동으로 나을 수 있고, {@code mismatched} 는 낫지 않는다
     * (토픽을 다시 만들거나 재배치해야 한다).
     */
    public String cause() {
        return state.get().cause();
    }

    private ProvisionOutcome tryProvision(int attempt) {
        try {
            ProvisionOutcome outcome = attemptProvisioning.get();
            if (outcome == ProvisionOutcome.PROVISIONED) {
                state.set(new Snapshot(SourceStatus.VALID, "none"));
                log.info("토픽 선언을 브로커에 반영했다 ({}회차)", attempt);
            } else {
                log.debug("토픽 확인 {}회차 — {}", attempt, outcome);
            }
            return outcome;
        } catch (Exception failure) {
            // 컨텍스트가 닫히는 중이면 여기로 온다. 확인을 못 한 것이지 불일치가 아니다.
            lastFailure.set(failure);
            log.debug("토픽 확인 {}회차 실패 — 재시도한다", attempt, failure);
            return ProvisionOutcome.UNCONFIRMED;
        }
    }

    private boolean sleep(Duration backoff) {
        try {
            Thread.sleep(backoff.toMillis());
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
