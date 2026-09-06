// 릴레이 워커가 접수 요청 몫의 DB 커넥션을 남겨 두는지 기동 때 확인합니다.
package com.kafkick.infra.mq.notification;

import javax.sql.DataSource;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b>릴레이 워커가 접수 요청의 커넥션을 다 가져가면 안 된다.</b>
 *
 * <p>이 릴레이는 <b>접수 API 프로세스 안에서</b> 돌고 커넥션 풀을 접수 요청과 <b>공유한다.</b>
 * 워커는 건당 커넥션을 <b>두 번</b> 빌린다 — 발행 전 {@code findById}, 발행 후
 * {@code markPublished} — 그 사이에 Kafka 왕복이 있다. 워커가 많아지면 그 빌림이 겹쳐
 * <b>접수 요청이 커넥션을 기다린다.</b>
 *
 * <h2>실측 — 절벽이 어디서 시작하나 (CY-923)</h2>
 *
 * <p>풀을 배포 기본값(13)으로 두고 워커만 올리며, 같은 JVM 의 요청 스레드 지연을 쟀다.
 *
 * <pre>
 *   워커  6   요청 p99  713µs
 *   워커  8   요청 p99  862µs      ← 기본값
 *   워커 10   요청 p99  1,343µs
 *   워커 11   요청 p99  999µs      ← 여기까지 평평하다
 *   워커 12   요청 p99  4,069µs    ← 4.1 배로 뛴다
 *   워커 13   요청 p99  3,689µs
 *   워커 16   요청 p99  6,606µs
 * </pre>
 *
 * <p><b>11 과 12 사이에서 갈린다.</b> 풀이 13 이므로 접수에 <b>둘</b>이 남는 데까지는
 * 평평하고, <b>하나</b>만 남으면 무너진다. 그래서 규칙이 {@value #REQUEST_HEADROOM} 이다 —
 * <b>지어낸 값이 아니라 잰 두 점을 정확히 가르는 값</b>이다.
 *
 * <p>⚠️ 한때 이 자리를 <b>CPU 초과 예약</b>으로 설명했다(코어 11, 절벽 16). 재 보니
 * 워커 32 에서도 프로세스 CPU 는 평균 8~9% 였고, <b>풀만 13 → 64 로 바꾸니 절벽이
 * 사라졌다.</b> 원인은 CPU 가 아니라 풀이다. 전체 표는
 * {@code docs/18-relay-throughput-measurement.md}.
 *
 * <h2>왜 기동을 거절하나 — {@link RelayBinlogFormatGuard} 는 WARN 인데</h2>
 *
 * <p>그쪽은 <b>운영 환경의 성질</b>(복제 형식)이라 이 프로세스가 고칠 수 없고, 막으면 접수
 * API 전체가 안 뜬다. 이쪽은 <b>우리 설정 둘 사이의 관계</b>다 — 배포하는 사람이 값을
 * 고치면 그 자리에서 풀린다. 그리고 걸렸을 때의 증상이 <b>접수 지연 네 배</b>(경계인
 * 워커 12) <b>에서 여덟 배</b>(워커 16)인데
 * 앱은 정상으로 보이므로, 조용히 뜨는 것이 더 나쁘다.
 *
 * <p><b>끄는 손잡이는 둔다.</b> {@link com.kafkick.infra.mq.config.NotificationRelayConfig}
 * 이 만드는 가드라 여기서 막으면 접수 API 가 안 뜬다 — 되돌릴 방법이 없으면 안 된다.
 * {@code DataSourceTimeoutGuard} 와 같은 모양이다.
 */
public class RelayWorkerPoolHeadroomGuard {

    private static final Logger log = LoggerFactory.getLogger(RelayWorkerPoolHeadroomGuard.class);

    /**
     * 접수 요청에 남겨야 하는 커넥션 수.
     *
     * <p><b>실측에서 나온 값이다.</b> 배포와 같은 조건(풀 13, <b>요청 스레드 15</b> —
     * {@code TOMCAT_THREADS_MAX} 기본값)에서 워커를 올리며 요청 지연을 쟀다(3회차).
     *
     * <pre>
     *   워커  8   p95 885 / 720 / 988µs     p99 1,496 / 1,157 / 2,529µs
     *   워커  9   p95 890 / 796 / 755µs     p99 1,695 / 1,225 / 1,288µs   ← 8 과 구분 안 됨
     *   워커 10   p95 1,681 / 921 / 1,376µs p99 5,451 / 1,595 / 2,872µs   ← 분산이 터진다
     *   워커 11   p95 1,666 / 1,591 / 2,125µs p99 3,783 / 3,019 / 3,540µs ← 일관되게 2~3배
     * </pre>
     *
     * <p>경계가 <b>9 와 10 사이</b>다. 풀 13 에서 워커 9 는 접수 몫 <b>4</b> 를 남긴다 —
     * 이 값이 잰 두 점을 정확히 가른다. 5 로 올리면 <b>멀쩡한 워커 9 를 거절</b>하고,
     * 3 으로 내리면 <b>분산이 터지는 워커 10 을 통과</b>시킨다.
     *
     * <p>⚠️ <b>한때 이 값이 2 였고, 그것도 "잰 값" 이었다.</b> 다만 잴 때의 요청 부하가
     * <b>프로브 스레드 하나</b>였다 — 배포는 톰캣 워커가 <b>15</b> 다. 요청 쪽 동시성을
     * 실제와 맞추니 경계가 11→12 에서 <b>9→10</b> 으로 앞당겨졌다. <b>재는 조건이 실제와
     * 다르면 "실측" 도 틀린다</b>(2026-09-05 잠금 용량 측정 보고서가 적어 둔 배포 수치
     * — API 2대·각 풀 13·톰캣 15 — 를 보고 알아챘다).
     *
     * <p>기본값은 통과한다 — {@code 8 + 4 = 12 ≤ 13}. <b>여유가 1 뿐인 것이 사실이다.</b>
     * {@code DB_POOL_SIZE} 를 12 로 내리면 기본 배포가 기동을 거부하는데, 그 조합은
     * <b>안 재 봤으므로</b> 막는 것이 맞다.
     */
    static final int REQUEST_HEADROOM = 4;

    /** 거절을 끄는 손잡이. 기본은 켬 — 끄는 법은 거절 메시지가 직접 말한다. */
    public static final String REQUIRED =
            "kafka.notification.relay.pool-headroom-guard.required";

    /**
     * 검사 결과를 싣는 게이지.
     *
     * <p><b>알림 규칙이 이 이름을 그대로 쓴다</b>({@code rules/outbox-alerts.yml} 의
     * {@code RelayPoolHeadroomUnverified}). 여기서 이름을 바꾸고 규칙을 안 고치면
     * Prometheus 는 에러가 아니라 <b>빈 결과</b>를 돌려주고 알림이 영원히 안 뜬다 —
     * 그리고 알림이 안 오는 것은 "정상" 과 구분되지 않는다.
     * {@code OutboxAlertRuleContractTest} 가 이 상수와 규칙 파일을 대조한다.
     */
    public static final String GAUGE = "cy_notify_relay_pool_headroom_verified";

    /** 게이지가 읽는 값. 생성자가 한 번만 도므로 필드에 남긴다. */
    private volatile boolean verified;

    public RelayWorkerPoolHeadroomGuard(DataSource dataSource, int workerCount, boolean required,
            MeterRegistry registry) {
        Integer maxPoolSize = maxPoolSizeOf(dataSource);
        if (maxPoolSize == null) {
            publish(registry, false);
            log.warn("DataSource 에서 최대 풀 크기를 못 읽어 워커/풀 검사를 건너뜁니다. "
                    + "getMaximumPoolSize() 가 없는 구현입니다. type={}",
                    dataSource.getClass().getName());
            return;
        }

        if (workerCount + REQUEST_HEADROOM > maxPoolSize) {
            reject(registry, required,
                    "릴레이 워커 수(" + workerCount + ")가 커넥션 풀(" + maxPoolSize
                            + ")에서 접수 몫 " + REQUEST_HEADROOM + " 을 남기지 못합니다. "
                            + "이 릴레이는 접수 API 와 같은 풀을 쓰고, 워커는 건당 커넥션을 "
                            + "두 번 빌립니다 — 남는 것이 하나뿐이면 접수 요청 p99 가 "
                            + "네 배가 되는데 앱은 정상으로 보입니다"
                            + "(실측: 풀 13 에서 워커 11 → 999µs, 워커 12 → 4,069µs. "
                            + "docs/18). 워커를 " + (maxPoolSize - REQUEST_HEADROOM)
                            + " 이하로 줄이거나 DB_POOL_SIZE 를 "
                            + (workerCount + REQUEST_HEADROOM) + " 이상으로 올리십시오.");
            return;
        }

        publish(registry, true);
        log.info("릴레이 워커/풀 확인 완료 — 워커 {} · 풀 {} · 접수 몫 {} 남음.",
                workerCount, maxPoolSize, maxPoolSize - workerCount);
    }

    /**
     * <b>끈 상태를 지표로 낸다.</b> 이 스택에는 Loki·promtail 이 없어 <b>로그가 감시 수단이
     * 아니다.</b> ERROR 한 줄로만 남기면 며칠 뒤 그 줄에 닿는 사람이 없다.
     */
    private void publish(MeterRegistry registry, boolean verified) {
        this.verified = verified;
        if (registry == null) {
            return;
        }
        Gauge.builder(GAUGE, this, self -> self.verified ? 1 : 0)
                .description("릴레이 워커가 접수 몫 커넥션을 남기는가 — 1 통과 · 0 못 했거나 껐다")
                .register(registry);
    }

    /** <b>기본은 거절, 끄면 ERROR.</b> 끈 상태는 조용하지 않게 남긴다. */
    private void reject(MeterRegistry registry, boolean required, String message) {
        if (required) {
            throw new IllegalStateException(message
                    + " 지금 당장 띄워야 하면 환경변수 "
                    + "RELAY_POOL_HEADROOM_GUARD_REQUIRED=false (또는 실행 인자 --"
                    + REQUIRED + "=false) 로 거절을 끌 수 있습니다.");
        }
        publish(registry, false);
        log.error("릴레이 워커/풀 검사에 걸렸습니다 — 거절은 꺼져 있습니다({}=false). {}",
                REQUIRED, message);
    }

    /**
     * <b>커넥션을 열지 않는다.</b> 풀은 설정값을 그대로 들고 있으므로 접속 없이 읽는다.
     *
     * <p><b>리플렉션인 이유.</b> {@code infra:mq} 의 컴파일 클래스패스에 Hikari 가 없다 —
     * JDBC 풀은 {@code storage} 가 소유하고 이 모듈은 {@link DataSource} 만 안다. 가드 하나
     * 때문에 의존을 늘려 <b>이 모듈이 풀 구현을 알게 되는</b> 쪽보다, 이름으로 찾고 못 찾으면
     * <b>WARN 으로 남기고 통과</b>시키는 쪽이 낫다. 여기서 기동을 막으면 풀 구현을 바꾸는 날
     * 접수 API 가 통째로 안 뜬다. {@code DataSourceTimeoutGuard#jdbcUrlOf} 와 같은 모양이다.
     */
    private static Integer maxPoolSizeOf(DataSource dataSource) {
        try {
            Object size = dataSource.getClass().getMethod("getMaximumPoolSize").invoke(dataSource);
            return size instanceof Integer value ? value : null;
        } catch (ReflectiveOperationException | RuntimeException notAPool) {
            return null;
        }
    }
}
