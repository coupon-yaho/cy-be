// 배치가 마지막으로 성공한 시각과, 진도가 멈춘 실행 수를 관제에 냅니다.
package com.kafkick.batch.config;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.batch.core.job.Job;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>"안 돌았다" 를 증분 창이 아니라 마지막 성공 시각으로 본다.</b>
 *
 * <p>예전 {@code BatchJobNotRunning} 은 {@code spring_batch_job_seconds_count} 의 <b>15분 창
 * 절대 델타</b>가 0 인지로 판정했다. 주기가 5분일 때만 성립하는 축이라 — 만료를 배치 창
 * (일 1회)으로 옮기면 그 창이 <b>하루의 대부분 비어</b> 영구 critical 이 된다.
 * 규칙이 틀린 게 아니라 <b>축이 안 맞았다.</b>
 *
 * <p><b>마지막 성공 시각은 그 축을 안 탄다.</b> 주기가 5분이든 하루든 <i>"마지막 성공이
 * 얼마나 됐나"</i> 하나로 같은 질문을 하고, SLA 만 주기에 맞춰 바꾸면 된다.
 *
 * <p><b>근거는 DB 다 — JVM 안 카운터가 아니다.</b> 그 카운터는 재기동에 새로 태어난다.
 * 5분 주기에서는 몇 분이면 값이 채워지지만 <b>일 1회에서는 최대 하루 동안 "모름"</b> 이고,
 * 그 사이 알림은 데이터가 아니라 배포 시각을 보고 운다. 배치 메타의
 * {@code BATCH_JOB_EXECUTION} 은 프로세스보다 오래 살아 그 구멍이 없다.
 *
 * <p><b>성공은 "판정을 냈다" 이지 "데이터가 깨끗하다" 가 아니다.</b> 검증이
 * {@code verdict=FAIL} 을 내도 잡은 {@code COMPLETED} 이고, 그것은 감시 축에서 <b>성공</b>이다 —
 * 이 저장소가 <i>"배치가 실패했다고 할 때는 판정을 내지 못한 경우뿐"</i> 이라고 정한 그대로다.
 * 데이터가 틀렸다는 사실은 {@code cy_verification_verdict} 가 따로 진다.
 *
 * <p><b>{@code verifyJob} 의 SLA 는 이 계열이 아니라 {@code cy_verify_last_success_seconds}
 * {@code {dataset,scope}} 가 진다</b>(CY-470, {@code docs/13} §6 의 D). 이 계열은 잡 이름
 * 그레인이라 {@code verifyJob} 하나로 {@code CLEAN/FULL}(게이트가 보는 것)과
 * {@code CORRUPT/FULL}(리허설)을 뭉치는데, 그러면 <b>리허설 한 번이 시계열을 앞으로 밀어</b>
 * SLA 를 리셋한다 — 정작 게이트 조합은 며칠째 안 돌았는데 조용하다.
 * {@code cy_verification_verdict} 가 이미 {@code (dataset, scope)} 그레인이라 두 축이 조인된다.
 *
 * <p>그레인을 가르는 조회는 {@code BatchRunMetricsRefresher#verifyLastSuccessEpochSeconds} 이고
 * {@code BATCH_JOB_EXECUTION_PARAMS} 를 조인한다. CY-392 가 이 계열만 미리 내 뒀던 것은
 * <b>D 가 감시를 나중에 붙이는 상태로 시작하지 않게</b> 하려던 것이다.
 *
 * <p>⚠️ <b>이 계열에 검증 SLA 를 다시 걸면 안 된다.</b> 두 번째 알림이 생겨 같은 사건이
 * 두 채널로 나가고, 그중 하나는 리허설에 리셋되는 거짓 축이다.
 */
@Component
public class BatchRunMetrics {

    /**
     * <b>{@code job} 을 쓰면 안 된다.</b> 그것은 프로메테우스가 스크레이프할 때 <b>자기가
     * 붙이는 타깃 라벨</b>이라({@code job_name: cy-batch}), {@code honor_labels} 가 꺼진
     * 기본값에서 우리 라벨이 {@code exported_job} 으로 <b>개명당한다.</b> 그러면 규칙의
     * {@code {job="expireJob"}} 은 아무 시계열과도 안 맞고, {@code absent()} 갈래가
     * <b>항상 참</b>이 되어 알림이 영구 발화한다 — 이 축이 없애려던 바로 그 상태다.
     *
     * <p>스프링 배치가 자기 태그를 {@code spring_batch_job_name} 으로 짓는 이유가 그것이고,
     * 같은 이름을 쓰면 기존 규칙과 라벨 축이 맞아 조인도 된다.
     */
    private static final String JOB_TAG = "spring_batch_job_name";

    /**
     * <b>검증 SLA 는 잡 이름 그레인으로 못 진다.</b> {@code verifyJob} 하나가
     * {@code CLEAN/FULL}(게이트가 보는 것)과 {@code CORRUPT/FULL}(리허설)을 함께 도는데,
     * 잡 이름만 태그로 쓰면 <b>{@code CORRUPT} 손 트리거 한 번이 같은 시계열을 앞으로 밀어
     * SLA 를 리셋한다</b> — 정작 {@code CLEAN/FULL} 은 며칠째 안 돌았는데 조용하다.
     * 그래서 이 게이지만 {@code (dataset, scope)} 축을 따로 가진다(CY-470).
     *
     * <p>{@code cy_verification_verdict} 가 이미 같은 그레인이라 두 축이 조인된다.
     */
    private static final String DATASET_TAG = "dataset";

    private static final String SCOPE_TAG = "scope";

    /** 관제가 "모른다" 를 0 으로 오해하지 않게 한다. 0 은 1970년이라 즉시 알림이 된다. */
    private static final double UNKNOWN = Double.NaN;

    private final AtomicReference<Map<String, Snapshot>> latest =
            new AtomicReference<>(Map.of());
    private final AtomicReference<Double> verifyLastSuccess =
            new AtomicReference<>(UNKNOWN);
    private final List<String> watchedJobNames;

    /**
     * <b>볼 잡을 {@code Job} 빈에서 직접 받는다.</b> 이름을 여기 리터럴로 적으면 CY-384 가
     * {@code JOB_NAME} 으로 없앤 이중화가 되살아나고, 설정 키로 빼면 오타가 <b>없는 잡을
     * 보는 상태</b>를 만든다. 빈에서 받으면 셋째 잡이 생기는 날 자동으로 따라온다.
     *
     * <p>이름 순으로 정렬한다 — 빈 순서는 보장이 없는데, 그것이 흔들리면 같은 배포에서
     * 태그 집합이 달라 보인다.
     */
    public BatchRunMetrics(MeterRegistry registry, List<Job> jobs) {
        this.watchedJobNames = jobs.stream().map(Job::getName).sorted().toList();

        for (String jobName : watchedJobNames) {
            gauge(registry, "cy_batch_last_success_seconds", jobName,
                    Snapshot::lastSuccessEpochSeconds,
                    "이 잡이 마지막으로 COMPLETED 로 끝난 시각(유닉스 초). 판정 결과와 무관하다");
            gauge(registry, "cy_batch_stuck_executions", jobName,
                    snapshot -> snapshot.stuckExecutions(),
                    "종료 표시 없이 실행 중으로 남았는데 진도가 멈춘 실행 수");
        }

        // 잡 유무와 무관하게 **언제나** 낸다. 위 루프처럼 Job 빈에서 이름을 받아 조건부로
        // 만들면, verifyJob 빈이 없는 기동에서 이 계열이 통째로 사라져 VerifyMetricsMissing
        // 이 "타깃은 살아 있는데 계열만 없다" 를 못 가린다 — CY-446 이 회차 카운터를
        // 무조건부 빈으로 옮긴 것과 같은 이유다.
        Gauge.builder("cy_verify_last_success_seconds", verifyLastSuccess,
                        AtomicReference::get)
                .tag(DATASET_TAG, VerifyRunContext.SLA_DATASET)
                .tag(SCOPE_TAG, VerifyRunContext.SLA_SCOPE)
                .description("이 (dataset, scope) 조합의 검증이 마지막으로 COMPLETED 로 끝난 "
                        + "시각(유닉스 초). 판정 결과와 무관하다")
                .register(registry);
    }

    /** 되읽기가 훑어야 하는 잡들. 이름 순이다. */
    public List<String> watchedJobNames() {
        return watchedJobNames;
    }

    /**
     * 되읽기가 성공했을 때 그 결과로 통째로 갈아 끼운다.
     *
     * <p><b>부분 갱신을 안 한다.</b> 잡 하나만 갱신하면 다른 잡의 값이 언제 것인지 알 수
     * 없어지는데, 그 상태는 <i>"낡았다"</i> 와 <i>"방금 읽었다"</i> 가 같은 모양이다.
     */
    public void record(Map<String, Snapshot> fresh) {
        latest.set(Map.copyOf(fresh));
    }

    /**
     * {@code (SLA_DATASET, SLA_SCOPE)} 검증의 마지막 성공 시각.
     *
     * <p><b>위 {@code record} 와 한 되읽기 안에서 함께 불려야 한다.</b> 따로 갱신하면
     * {@code cy_batch_last_success_seconds{verifyJob}} 과 이 값이 서로 다른 시점을 말하는
     * 구간이 생기는데, 관제에서 그 둘을 나란히 놓고 보는 것이 진단의 첫 단계다.
     *
     * @param epochSeconds 없으면 {@code NaN} — <i>"그 조합으로 성공한 실행이 아예 없다"</i>
     */
    public void recordVerifyLastSuccess(double epochSeconds) {
        verifyLastSuccess.set(epochSeconds);
    }

    /**
     * <b>못 읽었다는 것을 그대로 내보낸다.</b> 직전 값을 들고 있으면 관제는 그것을 지금
     * 상태로 읽는다 — 마지막 성공 시각에서 그 오해는 <b>"방금 돌았다"</b> 가 되어
     * SLA 알림을 통째로 재운다.
     */
    public void markUnknown() {
        latest.set(Map.of());
        // 검증 게이지도 함께 떨어뜨린다. 안 떨어뜨리면 되읽기가 죽은 동안 이 계열만
        // 마지막 값을 들고 있어, 관제가 두 축을 나란히 볼 때 **하나는 모름 하나는 정상**
        // 이라는 존재하지 않는 상태를 만든다.
        verifyLastSuccess.set(UNKNOWN);
    }

    /**
     * @param lastSuccessEpochSeconds 마지막 {@code COMPLETED} 의 종료 시각. 없으면 {@code NaN}
     * @param stuckExecutions         진도가 멈춘 실행 중 행의 수
     */
    public record Snapshot(double lastSuccessEpochSeconds, int stuckExecutions) {
    }

    private void gauge(MeterRegistry registry, String name, String jobName,
            java.util.function.ToDoubleFunction<Snapshot> field, String description) {
        Gauge.builder(name, latest, holder -> {
                    Snapshot snapshot = holder.get().get(jobName);
                    return snapshot == null ? UNKNOWN : field.applyAsDouble(snapshot);
                })
                .tag(JOB_TAG, jobName)
                .description(description)
                .register(registry);
    }
}
