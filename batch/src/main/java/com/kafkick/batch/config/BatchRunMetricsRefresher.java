// 배치 메타에서 마지막 성공 시각과 멈춘 실행 수를 주기로 되읽습니다.
package com.kafkick.batch.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * <b>게이지가 스크레이프 경로에서 DB 를 치지 않게 한다.</b> 미터의 람다는 프로메테우스가
 * 긁을 때마다 불리는데, 거기서 질의하면 DB 가 느린 날 <b>액추에이터 응답이 통째로 늘어진다</b> —
 * 그러면 지표를 못 읽는 것과 배치가 안 도는 것이 같은 모양이 된다. 그래서 주기로 읽어
 * 홀더에 넣고 게이지는 그것만 본다({@code VerificationMetricsRefresher} 와 같은 구조다).
 *
 * <p><b>에폭 변환을 MySQL 에게 맡긴다 — 자바에서 하면 안 된다.</b> 드라이버가 쓸 때
 * JVM 존의 값을 <b>DB 세션 존으로 옮겨</b> 저장하는데, 같은 값을 {@code JdbcClient} 로
 * 읽으면 그 변환을 <b>안 되돌린 채</b> 온다. 그것을 다시 JVM 존으로 해석하면 그 차이만큼
 * 어긋난다 — KST 기기에서 실측했다: 객체의 {@code endTime} 이 {@code 10:30} 인데 저장된
 * 값은 {@code 01:30} 이었고, 그것을 KST 로 읽어 <b>아홉 시간 과거</b>가 나왔다.
 * 그 방향이 <i>"아홉 시간 전에 성공했다"</i> 라 <b>알림이 늦게 운다.</b>
 *
 * <p>{@code UNIX_TIMESTAMP} 는 저장된 값을 <b>MySQL 세션 존으로</b> 해석한다.
 *
 * <p>⚠️ <b>"존이 무엇이든 상쇄된다" 가 아니다.</b> 드라이버는 URL 의
 * {@code serverTimezone}(= {@code connectionTimeZone}, 우리는 UTC 고정)으로 변환해 쓰고,
 * {@code UNIX_TIMESTAMP} 는 {@code @@session.time_zone} 으로 읽는다 — <b>서로 다른 설정</b>이다.
 * 상쇄는 그 둘이 같을 때만 성립한다. 그래서 {@code base.yml} 이 MySQL 을
 * {@code --default-time-zone=+00:00} 으로 띄운다. 그 한 줄을 빼면 세션 존이 호스트 존이 되고,
 * 게이지가 그 차이만큼 어긋나 {@code ExpireNotSucceeding} 이 영구 발화한다.
 *
 * <p>그래도 자바에서 변환하는 것보다 낫다 — 그쪽은 <b>세 축</b>(JVM 존 · 드라이버 존 ·
 * 세션 존)이 맞아야 하고, 이쪽은 <b>두 축</b>이면 된다. {@code RunningJobFixture} 가 진도를
 * 되돌릴 때 {@code DATE_SUB} 로 상대 시프트를 쓰는 것도 같은 이유다.
 *
 * <p><b>나이가 아니라 시각을 낸다.</b> {@code TIMESTAMPDIFF} 로 "얼마나 됐나" 를 내면
 * 되읽기가 멈춘 순간 그 값이 <b>얼어붙어 임계 아래에 머문다</b> — 감시가 꺼진 채 조용하다.
 * 시각을 내면 {@code time() - 게이지} 가 계속 자라 알림이 뜬다.
 *
 * <p><b>SQL 을 여기서 직접 친다 — 포트를 안 만든다.</b> 이 저장소의 "리포지토리 밖 raw SQL"
 * 규약은 <b>도메인 데이터</b>에 대한 것이다(같은 절의 나머지가 컨트롤러·서비스·엔티티·DTO·
 * Redis/Kafka 다). {@code BATCH_JOB_EXECUTION} 은 도메인이 아니라 <b>프레임워크 메타</b>라,
 * 그것을 {@code core} 의 포트 뒤로 넣으면 도메인이 스프링 배치 스키마를 알게 된다 —
 * 레이어링이 나아지는 것이 아니라 반대로 간다. 같은 패키지의 {@code BinlogFormatGuard} 도
 * 같은 이유로 {@code JdbcClient} 를 직접 쓴다.
 *
 * <p><b>{@code batch.scheduling.enabled} 에 묶지 않는다.</b> 그 스위치는 <i>원본을 쓰는 잡을
 * 돌릴 것인가</i> 를 정하는데 이것은 읽기만 한다. 묶으면 스케줄러를 끈 채 띄우는 기동에서
 * 지표가 통째로 죽고, 그때야말로 <i>"마지막으로 언제 돌았나"</i> 를 봐야 하는 자리다.
 */
@Component
public class BatchRunMetricsRefresher {

    /**
     * <b>되읽기 주기가 SLA 예산을 그대로 갉아먹는다.</b> 게이지는 <i>"마지막 되읽기 시점에
     * 알던 마지막 성공 시각"</i> 이지 실시간 값이 아니다. 알림이 {@code time() - 게이지 > 900}
     * 이므로 이 주기만큼 나이가 부풀고, 거기에 {@code max-expire-skips} 최대 지연(600초)이
     * 겹치면 <b>정상 상태에서 오탐 critical</b> 이 난다.
     *
     * <p>{@code .example} 값만 보는 테스트로는 운영에서 환경변수로 올리는 것을 못 잡는다 —
     * 그게 실제로 이 값이 커지는 경로다. {@code VerificationMetricsRefresher} 가 같은 이유로
     * 같은 모양의 상한을 둔다.
     */
    private static final long MAX_REFRESH_MILLIS = 120_000;

    /** 이보다 짧으면 배치 메타를 쉼 없이 친다. 얻는 것이 없다 — SLA 는 초 단위다. */
    private static final long MIN_REFRESH_MILLIS = 5_000;

    private static final Logger log = LoggerFactory.getLogger(BatchRunMetricsRefresher.class);

    private final JdbcClient jdbcClient;
    private final BatchRunMetrics metrics;
    private final RunningJobProbe runningJobs;

    /**
     * <b>못 읽은 횟수를 센다.</b> 게이지를 NaN 으로 떨어뜨리는 것만으로는 <b>간헐</b> 실패를
     * 못 잡는다 — 정상 ↔ NaN 을 오가면 알림의 {@code for} 타이머가 정상값이 돌아올 때마다
     * 리셋돼 <b>영영 안 찬다.</b> 되읽기가 절반쯤 망가진 상태가 관제에 정상으로 보인다.
     * {@code VerificationMetricsRefresher} 가 같은 구멍을 같은 방식으로 메웠다.
     */
    private final Counter refreshFailures;

    /** 연속 실패 횟수. 스택트레이스를 첫 회에만 남기는 데 쓴다. */
    private final AtomicLong failures = new AtomicLong();

    /**
     * <b>되읽기에 데드라인을 준다.</b> 이 조회는 스케줄러 스레드에서 도는데, 트랜잭션 밖이면
     * {@code DataSourceUtils} 가 {@code queryTimeout} 을 안 붙여 <b>끊을 수단이 없다.</b>
     * 커넥션 풀이 마르거나 배치 메타가 잠긴 날 이 스레드가 붙잡히면 다음 주기도 안 돌고,
     * 그때 게이지는 낡은 값을 든 채 조용하다. {@code VerificationMetricsRefresher} 가
     * 같은 이유로 같은 모양을 쓴다.
     */
    private final TransactionTemplate readLatest;

    public BatchRunMetricsRefresher(JdbcClient jdbcClient, BatchRunMetrics metrics,
            RunningJobProbe runningJobs, PlatformTransactionManager transactionManager,
            MeterRegistry registry,
            @Value("${batch.metrics.run-timeout-ms:5000}") long timeoutMillis,
            @Value("${batch.metrics.run-refresh-ms:60000}") long refreshMillis) {
        if (refreshMillis < MIN_REFRESH_MILLIS || refreshMillis > MAX_REFRESH_MILLIS) {
            throw new IllegalArgumentException(
                    "batch.metrics.run-refresh-ms 는 " + MIN_REFRESH_MILLIS + "~"
                            + MAX_REFRESH_MILLIS + " 이어야 합니다. "
                            + "게이지는 마지막 되읽기 시점의 값이라 이 주기가 "
                            + "ExpireNotSucceeding 의 SLA(900초) 예산을 그대로 갉아먹습니다. "
                            + "만료 슬롯 건너뛰기의 최대 지연(600초)과 겹치면 정상 상태에서 "
                            + "오탐 critical 이 납니다. 받은 값=" + refreshMillis);
        }
        if (timeoutMillis < 1_000 || timeoutMillis % 1_000 != 0) {
            // 스프링의 트랜잭션 타임아웃은 초 단위다. 1500 을 주면 1 로 잘려 **적은 값보다
            // 짧게** 끊기는데, 그 사실이 어디에도 안 남는다.
            throw new IllegalArgumentException(
                    "batch.metrics.run-timeout-ms 는 1000 이상이면서 1000 의 배수여야 합니다. "
                            + "받은 값=" + timeoutMillis);
        }
        this.jdbcClient = jdbcClient;
        this.metrics = metrics;
        this.runningJobs = runningJobs;
        this.refreshFailures = Counter.builder("cy_batch_refresh_failures_total")
                .description("배치 실행 지표 되읽기가 실패한 횟수. 간헐 실패는 게이지만으로 안 보인다")
                .register(registry);
        this.readLatest = new TransactionTemplate(transactionManager);
        this.readLatest.setReadOnly(true);
        this.readLatest.setTimeout(Math.toIntExact(timeoutMillis / 1_000));
    }

    /**
     * <b>한 잡이라도 못 읽으면 전부 "모름" 으로 낸다.</b> 절반만 갱신하면 나머지 값이 언제
     * 것인지 알 수 없어지는데, 마지막 성공 시각에서 그 오해는 <i>"방금 돌았다"</i> 가 되어
     * SLA 알림을 통째로 재운다. 못 읽었다는 사실이 값보다 중요하다.
     */
    @Scheduled(fixedDelayString = "${batch.metrics.run-refresh-ms:60000}")
    public void refresh() {
        try {
            Map<String, BatchRunMetrics.Snapshot> fresh = readLatest.execute(ignored -> {
                Map<String, BatchRunMetrics.Snapshot> read = new HashMap<>();
                for (String jobName : metrics.watchedJobNames()) {
                    read.put(jobName, new BatchRunMetrics.Snapshot(
                            lastSuccessEpochSeconds(jobName),
                            runningJobs.stuckExecutionCount(jobName)));
                }
                return read;
            });
            metrics.record(fresh == null ? Map.of() : fresh);
            failures.set(0);
        } catch (RuntimeException e) {
            // 여기서 안 떨어뜨리면 게이지가 직전 값을 그대로 들고 있어, 관제가 그것을
            // 지금 상태로 읽는다 — 마지막 성공 시각에서 그 오해는 "방금 돌았다" 다.
            markFailed(e);
        } catch (Error e) {
            // Error 도 게이지는 떨어뜨린다. 안 떨어뜨리면 stuck_executions 가 0 에 얼어붙어
            // BatchStuckExecution 이 Error 경로에서만 통째로 꺼진다 — 하필 시체 실행이
            // 생기기 쉬운 상황(메모리 압박·풀 고갈)에서 그렇게 된다.
            //
            // 다시 던지는 것은 제어 흐름을 위해서가 아니다(스프링의 스케줄러 핸들러가
            // 삼킨다). Error 를 이 자리에서 정상 종료로 접지 않겠다는 표시다.
            markFailed(e);
            throw e;
        }
    }

    /**
     * <b>같은 원인이 이어지면 스택트레이스를 매번 남기지 않는다.</b> 60초 주기라 DB 가 30분
     * 잠기면 트레이스가 서른 개 쌓이고, 그것이 알림 runbook 이 가리키는
     * <i>"만료 슬롯을 건너뜁니다"</i> WARN 을 밀어낸다.
     * {@code VerificationMetricsRefresher} 가 같은 이유로 같은 모양을 쓴다.
     */
    private void markFailed(Throwable cause) {
        metrics.markUnknown();
        refreshFailures.increment();
        long streak = failures.incrementAndGet();
        if (streak == 1) {
            log.warn("배치 실행 지표를 되읽지 못했습니다. 게이지를 NaN 으로 둡니다.", cause);
        } else {
            log.warn("배치 실행 지표를 계속 되읽지 못하고 있습니다. 연속={} 원인={}",
                    streak, cause.toString());
        }
    }

    /**
     * 마지막으로 {@code COMPLETED} 로 끝난 실행의 종료 시각.
     *
     * <p><b>판정 결과를 안 본다.</b> 검증이 {@code verdict=FAIL} 을 내도 잡은 성공이다 —
     * 이 축이 묻는 것은 <i>"돌긴 했나"</i> 이고, 데이터가 틀렸다는 사실은
     * {@code cy_verification_verdict} 가 따로 진다.
     *
     * <p>{@code END_TIME} 이 비어 있는 행은 창 조건이 함께 걸러 준다 — {@code NULL} 과의
     * 비교는 참이 아니기 때문이다. {@code COMPLETED} 인데 종료 시각이 없는 것은 배치 메타가
     * 깨진 것인데, 그런 행이 {@code MAX} 에 섞이면 <b>더 오래된 실행이 마지막 성공으로
     * 보인다</b> — 창이 그 자리를 겸한다.
     */
    private double lastSuccessEpochSeconds(String jobName) {
        Optional<Double> epochSeconds = jdbcClient.sql("""
                        SELECT FLOOR(UNIX_TIMESTAMP(MAX(e.END_TIME)))
                          FROM BATCH_JOB_EXECUTION e
                          JOIN BATCH_JOB_INSTANCE i ON i.JOB_INSTANCE_ID = e.JOB_INSTANCE_ID
                         WHERE e.STATUS = 'COMPLETED'
                           AND e.END_TIME > DATE_SUB(NOW(), INTERVAL 7 DAY)
                           AND i.JOB_NAME = :jobName
                        """)
                .param("jobName", jobName)
                .query(Double.class)
                .optional();
        // 창 밖은 어차피 SLA 위반이라 NaN 과 같은 판정이다. 창을 두는 이유는 이 질의가
        // BATCH_JOB_INSTANCE 이력 전체에 비례해 자라는 것을 막기 위해서다 — 배치 메타를
        // 정리하는 잡이 아직 없고(docs/13 §6), 만료는 하루 288 인스턴스를 만든다.
        // V14 의 (STATUS, END_TIME) 인덱스가 이 창을 range 스캔으로 만든다 — 90일치에서
        // 25,950행 → 2,016행으로 줄어드는 것을 EXPLAIN 으로 쟀다. 선두 컬럼이 인스턴스
        // 쪽이면 창이 아무 효과가 없다(그렇게 썼다가 재보고 고쳤다).
        // 그래서 WHERE 절 순서도 실행 쪽이 먼저다 — 인스턴스는 PK 로 되짚는다.
        // ⚠️ SLA 보다 넉넉히 커야 한다. C 가 만료를 일 1회로 옮기면 이 창도 함께 본다.
        //
        // FLOOR 로 자르는 것은 UNIX_TIMESTAMP 가 마이크로초 소수부를 주기 때문이다.
        // 이 값은 프로메테우스의 time() 과 빼는 데만 쓰는데 그쪽이 초 해상도라,
        // 소수부는 정밀도가 아니라 잡음이다.

        // 한 번도 성공한 적이 없으면 0 이 아니라 NaN 이다. 0 은 1970년이라
        // "아주 오래 안 돌았다" 가 되어, 갓 뜬 서버에서 곧바로 알림이 된다.
        return epochSeconds.orElse(Double.NaN);
    }
}
