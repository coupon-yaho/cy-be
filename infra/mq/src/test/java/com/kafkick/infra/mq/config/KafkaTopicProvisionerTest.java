package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.function.Supplier;

import com.kafkick.core.observation.SourceStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

/**
 * 프로비저너 <b>본체</b>를 직접 부른다.
 *
 * <p>이전에는 이 코드를 실행하는 테스트가 저장소에 없었다 — {@code ApplicationContextRunner} 는
 * {@code ApplicationRunner} 를 호출하지 않고, 빈 유무만 보는 테스트는 람다 안에서 무슨 일이
 * 나든 초록불이다. 1차 리뷰의 핵심 수정(기동 15초 → 0.3초)이 들어간 블록이 미실행이었다.
 */
@ExtendWith(OutputCaptureExtension.class)
class KafkaTopicProvisionerTest {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void shutdownExecutor() {
        executor.shutdownNow();
    }

    /**
     * <b>실패의 종류에 따라 재시도의 의미가 다르다.</b> 확인하지 못한 것은 다시 해 볼 가치가 있고,
     * 선언과 다른 것은 재시도가 고치지 못한다 — 복제본 수는 만들 때만 정해진다.
     *
     * <p>이 구분을 예전에는 "브로커에 닿았는가" 로 추측했는데 틀렸다. 롤링 재기동 중 한 대가 아직
     * 안 올라왔으면 RF3 을 만족 못 해 생성이 실패하지만, 남은 두 대가 응답하므로 "닿았다" 로 읽힌다.
     * 그 상태에서 재시도를 끊으면 토픽이 없는 채로 발급이 시작된다.
     */
    @Test
    @DisplayName("선언과 다르면 재시도하지 않는다 — 재시도가 고치지 못한다")
    void stopsImmediatelyWhenTheDeclarationDoesNotMatch() {
        AtomicInteger calls = new AtomicInteger();
        KafkaTopicProvisioner provisioner = provisioner(() -> {
            calls.incrementAndGet();
            return ProvisionOutcome.MISMATCHED;
        }, 5);

        provisioner.provisionOnce();

        assertThat(calls.get()).as("45초를 태워도 낫지 않는다").isEqualTo(1);
        assertThat(provisioner.status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 반대로 "확인 못 함" 은 재시도해야 한다 — 브로커가 늦게 뜨는 것이 가장 흔한 경우다. */
    /**
     * 브로커 3대를 롤링 재기동하는 중에 배포되면 초반 시도는 전부 실패한다. 예전에는 5회(45초)를
     * 소진하고 <b>영영 포기</b>했다 — 그 인스턴스는 재기동 전까지 토픽 없이 트래픽을 받는다.
     */
    @Test
    @DisplayName("브로커가 늦게 떠도 결국 반영한다 — 포기하지 않는다")
    void keepsTryingUntilTheBrokerFinallyAnswers() {
        AtomicInteger calls = new AtomicInteger();
        KafkaTopicProvisioner provisioner = provisioner(
                () -> calls.incrementAndGet() >= 8
                        ? ProvisionOutcome.PROVISIONED : ProvisionOutcome.UNCONFIRMED, 5);

        provisioner.provisionOnce();

        assertThat(calls.get()).as("5회에서 끊기면 8번째 성공에 도달하지 못한다").isEqualTo(8);
        assertThat(provisioner.status()).isEqualTo(SourceStatus.VALID);
    }

    /** 계속 시도하더라도 경보는 걸려야 한다 — 아무도 모르게 오래 실패하면 안 된다. */
    @Test
    @DisplayName("재시도를 이어가는 중에도 일정 횟수 뒤에는 경보 상태가 된다")
    void alertsWhileStillRetrying() {
        AtomicInteger calls = new AtomicInteger();
        KafkaTopicProvisioner provisioner = provisioner(
                () -> calls.incrementAndGet() >= 9
                        ? ProvisionOutcome.PROVISIONED : ProvisionOutcome.UNCONFIRMED, 5);

        provisioner.provisionOnce();

        assertThat(calls.get()).isEqualTo(9);
        assertThat(provisioner.status())
                .as("성공했으니 최종은 VALID 다 — 중간에 UNAVAILABLE 을 거쳤어도")
                .isEqualTo(SourceStatus.VALID);
    }

    @Test
    @DisplayName("확인하지 못한 것은 끝까지 재시도한다")
    void keepsRetryingWhileUnconfirmed() {
        AtomicInteger calls = new AtomicInteger();
        KafkaTopicProvisioner provisioner = provisioner(
                () -> calls.incrementAndGet() >= 3
                        ? ProvisionOutcome.PROVISIONED : ProvisionOutcome.UNCONFIRMED, 5);

        provisioner.provisionOnce();

        assertThat(calls.get()).isEqualTo(3);
        assertThat(provisioner.status()).isEqualTo(SourceStatus.VALID);
    }

    /**
     * 원인이 로그에 안 실리면 경보 시점에 남는 것이 <b>횟수뿐</b>이다. 권한(ACL) 누락이나
     * 잘못된 호스트명처럼 <b>재기동으로 낫지 않는</b> 실패도 "브로커가 뜬 뒤 재기동하라" 로
     * 안내되고, 운영 지침이 로그로 원인을 가르라고 적어 둔 장치가 무력해진다.
     */
    @Test
    @DisplayName("경보를 걸 때 마지막 실패 원인이 로그에 남는다")
    void theFinalWarningCarriesTheCause(CapturedOutput output) {
        AtomicInteger calls = new AtomicInteger();
        // 3회째에 MISMATCHED 로 끝낸다 — 이제 재시도는 무한이라 종료 조건이 필요하다.
        KafkaTopicProvisioner provisioner = provisioner(() -> {
            if (calls.incrementAndGet() >= 3) {
                return ProvisionOutcome.MISMATCHED;
            }
            throw new IllegalStateException("토픽 생성 권한이 없다");
        }, 2);

        provisioner.provisionOnce();

        assertThat(output)
                .as("횟수만 남으면 다음 사람이 브로커 상태만 확인하고 같은 자리를 맴돈다")
                .contains("토픽 생성 권한이 없다");
    }

    @Test
    @DisplayName("브로커에 못 닿아도 예외를 밖으로 내지 않는다")
    void neverThrowsWhenTheBrokerIsUnreachable() {
        AtomicInteger calls = new AtomicInteger();
        KafkaTopicProvisioner provisioner = provisioner(() -> {
            if (calls.incrementAndGet() >= 2) {
                return ProvisionOutcome.MISMATCHED;
            }
            throw new IllegalStateException("브로커 미접속");
        }, 1);

        assertThatCode(provisioner::provisionOnce).doesNotThrowAnyException();
        assertThat(provisioner.isProvisioned())
                .as("확인되지 않았는데 확인된 것처럼 두면 계약 위반이 조용해진다")
                .isFalse();
        assertThat(provisioner.status())
                .as("실패를 확인한 것과 아직 시도 전인 것은 다른 상태다 — 둘 다 0 이면 경보를 못 건다")
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(provisioner.provisionedValue())
                .as("확인 못 한 값에 0 을 실으면 '확인했는데 0' 과 구분되지 않는다")
                .isNaN();
    }

    @Test
    @DisplayName("한 번 실패해도 다시 시도한다 — 브로커 롤링 재기동과 배포가 겹칠 수 있다")
    void retriesUntilTheBrokerAnswers() {
        AtomicInteger calls = new AtomicInteger();
        KafkaTopicProvisioner provisioner = provisioner(() -> calls.incrementAndGet() >= 3
                ? ProvisionOutcome.PROVISIONED : ProvisionOutcome.UNCONFIRMED, 5);

        provisioner.provisionOnce();

        assertThat(calls.get()).isEqualTo(3);
        assertThat(provisioner.isProvisioned()).isTrue();
        assertThat(provisioner.status()).isEqualTo(SourceStatus.VALID);
        assertThat(provisioner.provisionedValue()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("시도하기 전에는 실패가 아니라 '아직 확인 전' 이다")
    void startsAsPendingNotFailed() {
        KafkaTopicProvisioner provisioner = provisioner(() -> ProvisionOutcome.PROVISIONED, 1);

        assertThat(provisioner.status())
                .as("방금 뜬 인스턴스와 5회 실패한 인스턴스가 같은 값이면 롤링 배포마다 오탐이 난다")
                .isEqualTo(SourceStatus.PENDING);
    }

    /**
     * 지표만 보고 "재기동하면 낫는 실패" 와 "재기동해도 안 낫는 실패" 를 갈라야 한다.
     * 이 스택에는 로그를 모아 보는 도구가 없어서, 로그로 가르라는 지침은 화면에서 닿지 않는다.
     */
    @Test
    @DisplayName("마지막 실패의 종류를 꼬리표로 남긴다")
    void exposesTheCauseOfTheLastFailure() {
        assertThat(provisioner(() -> ProvisionOutcome.PROVISIONED, 1).cause())
                .as("아직 실패한 적이 없다")
                .isEqualTo("none");

        KafkaTopicProvisioner mismatched = provisioner(() -> ProvisionOutcome.MISMATCHED, 1);
        mismatched.provisionOnce();
        assertThat(mismatched.cause()).isEqualTo("mismatched");
    }

    @Test
    @DisplayName("성공하면 더 시도하지 않는다")
    void stopsOnceProvisioned() {
        AtomicInteger calls = new AtomicInteger();
        KafkaTopicProvisioner provisioner = provisioner(() -> {
            calls.incrementAndGet();
            return ProvisionOutcome.PROVISIONED;
        }, 5);

        provisioner.provisionOnce();

        assertThat(calls.get()).isEqualTo(1);
    }

    /**
     * 기동 스레드가 프로비저닝을 기다리면 안 된다. 기다리는 순간 이 클래스가 존재할 이유가
     * 사라진다 — 브로커 없는 회차에서 인스턴스마다 기동이 그만큼 늦어진다.
     */
    @Test
    @DisplayName("run() 은 작업을 넘기고 즉시 돌아온다")
    void runHandsOffAndReturnsImmediately() throws Exception {
        var executor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        try {
            KafkaTopicProvisioner provisioner = new KafkaTopicProvisioner(() -> {
                sleepQuietly();
                return ProvisionOutcome.PROVISIONED;
            }, executor, 1, Duration.ZERO);

            long startedAt = System.nanoTime();
            provisioner.run(null);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

            assertThat(elapsed).isLessThan(Duration.ofMillis(500));
        } finally {
            executor.shutdownNow();
        }
    }

    private static void sleepQuietly() {
        try {
            Thread.sleep(3_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 실행기를 호출마다 새로 만들면 닫는 자리가 없어진다. 지금 테스트들은 {@code provisionOnce()}
     * 를 직접 부르므로 실행기를 쓰지 않지만, 나중에 {@code run()} 경로 테스트가 추가되면 백오프
     * 중인 스레드가 남아 Gradle 워커가 종료를 기다린다.
     */
    /**
     * 두 미터는 각각 {@code status()} 와 {@code cause()} 를 <b>따로</b> 읽는다. 둘이 원자적으로
     * 움직이지 않으면 한 scrape 안에 존재하지 않는 조합이 잡힌다 — 예를 들어
     * {@code state=PENDING}("그대로 두면 낫는다") 인데 {@code cause=mismatched}("재기동으로
     * 안 낫는다") 다. 운영 지침이 두 시계열을 함께 읽으라고 적어 두어서 그 조합이 곧 오진이 된다.
     */
    @Test
    @DisplayName("상태와 원인은 한 번에 같이 바뀐다")
    void statusAndCauseMoveTogether() {
        KafkaTopicProvisioner provisioner = provisioner(() -> ProvisionOutcome.MISMATCHED, 1);

        provisioner.provisionOnce();

        assertThat(provisioner.snapshot().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(provisioner.snapshot().cause())
                .as("스냅샷 하나로 읽어야 두 값이 어긋난 조합이 아예 안 생긴다")
                .isEqualTo("mismatched");
    }

    /**
     * {@code submit} 은 {@code execute} 와 달리 예외를 Future 에 가둔다. 그 Future 를 버리면
     * {@code Error} 로 태스크가 죽어도 로그조차 안 남고, 상태는 {@code PENDING} 에 영구히
     * 머문다 — PENDING 은 "아직 확인 전, 곧 나아진다" 로 읽히기로 못박아 둔 값이라 경보가 안 간다.
     */
    @Test
    @DisplayName("태스크가 Error 로 죽어도 상태가 PENDING 에 남지 않는다")
    void aDeadTaskDoesNotLookLikePending() {
        KafkaTopicProvisioner provisioner = provisioner(() -> {
            throw new NoClassDefFoundError("org/apache/kafka/clients/admin/AdminClient");
        }, 1);

        assertThatThrownBy(provisioner::provisionOnce).isInstanceOf(NoClassDefFoundError.class);

        assertThat(provisioner.status())
                .as("죽은 태스크가 PENDING 이면 아무도 경보를 못 건다 — 그동안 RF1 토픽이 생긴다")
                .isEqualTo(SourceStatus.UNAVAILABLE);
    }

    private KafkaTopicProvisioner provisioner(
            Supplier<ProvisionOutcome> attempt, int maxAttempts) {
        return new KafkaTopicProvisioner(attempt, executor, maxAttempts, Duration.ofMillis(1));
    }
}
