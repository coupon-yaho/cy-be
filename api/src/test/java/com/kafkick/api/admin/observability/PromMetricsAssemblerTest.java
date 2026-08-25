package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorClass;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorClassKey;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorMetrics;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyGroupStat;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyMetrics;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyPercentiles;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TopReason;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TrafficKey;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.api.observation.http.HttpMetricsFilter.UriGroup;
import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;
import com.kafkick.core.support.TimeProvider;

/** Prometheus 표본이 값·상태·관측 시각으로 조립되는 규칙을 검증합니다. */
class PromMetricsAssemblerTest {

    private static final Instant NOW = Instant.parse("2026-08-21T00:00:00Z");
    /** instant query 가 표본에 붙여 주는 시각. <b>질의 평가 시각이지 관측 시각이 아니다.</b> */
    private static final Instant EVALUATED = NOW;
    private static final TimeProvider FIXED_TIME = new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC));
    private static final Duration STALE_AFTER = Duration.ofSeconds(120);
    private static final Duration BUDGET = Duration.ofMillis(900);

    /** 조립기가 응답 한 장에 보내는 질의 수. 예산이 넉넉하면 여섯 개가 모두 나간다. */
    private static final long QUERY_COUNT = 6;
    private static final long QUERY_DELAY_MILLIS = 20;

    private static final long FRESH_AGE_SECONDS = 3;
    private static final long STALE_AGE_SECONDS = 300;

    // ── meta ───────────────────────────────────────────────────────────────────

    /** 창 경계는 관측 시각과 집계 창에서 나오고, 창이 바뀌면 시작 시각도 바뀝니다. */
    @Test
    @DisplayName("meta 의 창 경계는 관측 시각과 집계 창을 따른다")
    void metaCarriesWindowBounds() {
        AdminMetricsResponse response = assemble(FakePromQuery.empty(), globalQuery());

        assertThat(response.meta().schemaVersion()).isEqualTo(1);
        assertThat(response.meta().snapshotAt()).isEqualTo(response.snapshotAt());
        assertThat(response.meta().windowEnd()).isEqualTo(response.snapshotAt());
        assertThat(response.meta().windowStart())
                .isEqualTo(response.snapshotAt().minus(Duration.ofMinutes(1)));
        assertThat(response.meta().sources()).isEmpty();

        AdminMetricsResponse wider = assemble(
                FakePromQuery.empty(), new MetricsQuery(MetricsWindow.FIFTEEN_MINUTES, null, null));
        assertThat(wider.meta().windowStart())
                .isEqualTo(wider.snapshotAt().minus(Duration.ofMinutes(15)));
    }

    /**
     * 시계는 고정돼 있어도 조립 시간은 실측이어야 합니다 — 벽시계로 재면 이 값이 0 으로 굳어
     * 예산에 얼마나 근접했는지가 보이지 않습니다.
     */
    @Test
    @DisplayName("collectionDurationMs 는 고정 시계와 무관하게 실제 경과를 잰다")
    void metaMeasuresCollectionDuration() {
        PromQuery slow = promQl -> {
            try {
                Thread.sleep(QUERY_DELAY_MILLIS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return List.of();
        };

        AdminMetricsResponse response = assemble(slow, globalQuery());

        // 질의 하나가 아니라 여섯 개를 합친 시간이어야 한다. 하한만 보므로 CI 가 느릴수록
        // 더 확실히 통과한다 — 느려서 깨질 수 있는 상한 단언이 아니다.
        assertThat(response.meta().collectionDurationMs())
                .isGreaterThanOrEqualTo(QUERY_DELAY_MILLIS * QUERY_COUNT);
    }

    // ── 값이 없을 때 ────────────────────────────────────────────────────────────

    /** 표본이 없는 구간은 0 이 아니라 PENDING 입니다. 0 은 "정상인데 0" 과 구분되지 않습니다. */
    @Test
    @DisplayName("표본이 하나도 없으면 모든 값이 PENDING 이고 0을 만들지 않는다")
    void emptySamplesYieldPending() {
        AdminMetricsResponse response = assemble(FakePromQuery.empty(), globalQuery());

        assertThat(response.traffic().issueAttemptRps().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.traffic().issueAttemptRps().value()).isNull();
        assertThat(response.latency().success().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.consistency().phase()).isEqualTo(ConsistencyPhase.LIVE);
        assertThat(response.consistency().verdict()).isNull();
        assertThat(response.snapshotAt()).isEqualTo(NOW);
    }

    /** Prometheus 가 죽어도 화면 전체가 죽으면 안 됩니다. */
    @Test
    @DisplayName("질의가 실패하면 예외가 아니라 UNAVAILABLE 로 나간다")
    void queryFailureBecomesUnavailable() {
        AdminMetricsResponse response = assemble(FakePromQuery.down(), globalQuery());

        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(response.latency().success().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(response.consistency().overIssued().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(response.consistency().severity()).isNull();
    }

    /**
     * 관측 시각을 모르면 값을 못 냅니다. 평가 시각으로 채우면 죽은 원천이 매 폴링마다 새로
     * 관측된 것처럼 보입니다.
     */
    @Test
    @DisplayName("신선도 미터가 없으면 값이 있어도 PENDING 이다")
    void unknownObservationTimeYieldsPending() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "app_consistency_gap_state", List.of(gap("lua", 3d), gapState("lua", SourceStatus.VALID))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.PENDING);
    }

    // ── 질의 ────────────────────────────────────────────────────────────────────

    /** 지표마다 한 번씩 부르면 1 초 폴링이 Prometheus 를 초당 수십 번 두드립니다. */
    @Test
    @DisplayName("셀렉터로 묶어 응답 한 장에 질의를 여섯 번만 보낸다")
    void usesSelectorsToLimitQueries() {
        FakePromQuery client = FakePromQuery.empty();
        assemble(client, globalQuery());

        assertThat(client.queries()).hasSize(6);
        assertThat(client.queries()).noneMatch(q -> q.contains("query_range"));
        // window 는 되돌아볼 범위가 아니라 비율을 계산할 집계 창이라 질의 안에 들어간다.
        assertThat(client.queries()).anyMatch(q -> q.contains("[60s]"));
        // 관측 시각은 표본이 아니라 timestamp() 로 따로 묻는다.
        assertThat(client.queries()).anyMatch(q -> q.contains("timestamp("));
    }

    /** 백분위 질의에는 창이 들어가지 않습니다 — expiry 가 정하는 값이라 PromQL 로 못 바꿉니다. */
    @Test
    @DisplayName("window 를 바꿔도 백분위 질의는 그대로다")
    void windowDoesNotReachPercentileQuery() {
        FakePromQuery oneMinute = FakePromQuery.empty();
        FakePromQuery fifteen = FakePromQuery.empty();
        assemble(oneMinute, globalQuery());
        assemble(fifteen, new MetricsQuery(MetricsWindow.FIFTEEN_MINUTES, null, null));

        assertThat(percentileQuery(oneMinute)).isEqualTo(percentileQuery(fifteen));
        assertThat(rateQuery(oneMinute)).isNotEqualTo(rateQuery(fifteen));
    }

    // ── 처리량 ──────────────────────────────────────────────────────────────────

    /** 결과 Counter 는 인스턴스 값을 더합니다. 발급 경로는 uri 그룹으로 먼저 쪼갠 뒤 집계합니다. */
    @Test
    @DisplayName("결과 rate 는 uri 그룹으로 쪼갠 뒤 인스턴스를 합산한다")
    void resultRatesSumAcrossInstances() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(
                        rate("issue", "success", 40d, "api-1"),
                        rate("issue", "success", 60d, "api-2"),
                        rate("queue", "success", 900d, "api-1"),
                        rate("issue", "policy_reject", 5d, "api-1"),
                        rate("entry", "queue_accepted", 7d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.traffic().issueSuccessTps().value()).isEqualTo(100d);
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.VALID);
        // 순번 폴링(queue 그룹)이 발급 처리량에 섞이면 안 된다.
        assertThat(response.traffic().issueAttemptRps().value()).isEqualTo(105d);
        assertThat(response.traffic().queueAcceptedRps().value()).isEqualTo(7d);
        // 관측 시각은 평가 시각이 아니라 마지막 스크레이프 시각이다.
        assertThat(response.traffic().issueSuccessTps().observedAt())
                .isEqualTo(EVALUATED.minusSeconds(FRESH_AGE_SECONDS));
    }

    /** 요청이 없는 것은 장애가 아니라 상태입니다. */
    @Test
    @DisplayName("그룹 전체 rate 가 0이면 NO_TRAFFIC 으로 나간다")
    void zeroTotalRateIsNoTraffic() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(rate("issue", "success", 0d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.traffic().issueAttemptRps().state()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.NO_TRAFFIC);
    }

    /**
     * 하위 분류가 0 인 것은 "요청이 없었다" 가 아니라 "그 결과가 한 건도 없었다" 입니다.
     * 둘을 같은 상태로 내보내면 초당 5,000 건이 전부 성공 중일 때 실패 패널만 회색으로 죽습니다.
     */
    @Test
    @DisplayName("트래픽이 있는데 하위 분류만 0이면 NO_TRAFFIC 이 아니라 VALID 다")
    void zeroSubclassWithTrafficIsValid() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(
                        rate("issue", "success", 5000d, "api-1"),
                        rate("issue", "application_failure", 0d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.traffic().issueAttemptRps().state()).isEqualTo(SourceStatus.VALID);
        assertThat(response.traffic().systemFailureRps().value()).isEqualTo(0d);
        assertThat(response.traffic().systemFailureRps().state()).isEqualTo(SourceStatus.VALID);
    }

    /** 스크레이프가 멈춰도 instant query 는 5분간 값을 계속 돌려줍니다. */
    @Test
    @DisplayName("마지막 스크레이프가 오래되면 값을 싣되 STALE 로 내려보낸다")
    void oldScrapeIsStale() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(STALE_AGE_SECONDS))));

        var success = assemble(client, globalQuery()).traffic().issueSuccessTps();

        assertThat(success.state()).isEqualTo(SourceStatus.STALE);
        assertThat(success.value()).isEqualTo(40d);
        assertThat(success.observedAt()).isEqualTo(EVALUATED.minusSeconds(STALE_AGE_SECONDS));
    }

    /**
     * 경계에서 부호가 바뀌어도(> 가 >= 로) 알아채야 합니다. 3초와 300초만 보면 그 변화가 안 잡힙니다.
     */
    @Test
    @DisplayName("나이가 stale-after 와 정확히 같으면 아직 STALE 이 아니다")
    void ageExactlyAtThresholdIsNotStaleYet() {
        assertThat(trafficStateAtAge(STALE_AFTER.toSeconds())).isEqualTo(SourceStatus.VALID);
        assertThat(trafficStateAtAge(STALE_AFTER.toSeconds() + 1)).isEqualTo(SourceStatus.STALE);
    }

    private static SourceStatus trafficStateAtAge(long ageSeconds) {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(ageSeconds))));
        return assemble(client, globalQuery()).traffic().issueSuccessTps().state();
    }

    /**
     * 한 응답에 질의가 다섯이라 질의별 타임아웃만으로는 응답이 폴링 간격을 넘길 수 있습니다.
     * 예산을 넘기면 남은 질의를 <b>보내지 않고</b> 그 값만 비웁니다.
     */
    @Test
    @DisplayName("응답 예산을 넘기면 남은 질의를 보내지 않고 UNAVAILABLE 로 내려보낸다")
    void exhaustedBudgetSkipsRemainingQueries() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d), gapState("lua", SourceStatus.VALID),
                        collectSuccess(FRESH_AGE_SECONDS)),
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = new PromMetricsAssembler(
                client, FIXED_TIME, STALE_AFTER, Duration.ofNanos(1)).assemble(globalQuery());

        // 예산이 1ns 라 첫 질의만 나가고 나머지는 잘린다.
        assertThat(client.queries()).hasSize(1);
        // 순서가 우선순위다 — 합격 판정을 가르는 정합성이 먼저 나간다.
        assertThat(client.queries().get(0)).contains("app_consistency_gap_state");
        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.VALID);
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(response.latency().success().state()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    // ── 실패 ────────────────────────────────────────────────────────────────────

    /**
     * 실패율은 건수가 아니라 비율입니다. 분모는 {@code issueAttemptRps} 하나로 고정하고,
     * 그 값은 트래픽이 쓰는 것과 <b>같은 표본</b>에서 나와야 합니다.
     */
    @Test
    @DisplayName("실패율은 같은 스냅샷의 issueAttemptRps 를 분모로 백분율을 낸다")
    void errorRatesDivideByTheSameAttemptSnapshot() {
        FakePromQuery client = respond(Map.of(
                "rate(app_http", List.of(
                        rate("issue", "success", 800d, "api-1"),
                        rate("issue", "dependency_failure", 10d, "api-1"),
                        rate("issue", "application_failure", 30d, "api-1"),
                        rate("issue", "client_invalid", 60d, "api-1"),
                        rate("issue", "policy_reject", 100d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());
        ErrorMetrics errors = response.errors();

        assertThat(response.traffic().issueAttemptRps().value()).isEqualTo(1000d);
        assertThat(errors.denominator()).isEqualTo(TrafficKey.ISSUE_ATTEMPT_RPS);
        assertThat(rateOf(errors, ErrorClassKey.DEPENDENCY_FAILURE).value()).isEqualTo(1d);
        assertThat(rateOf(errors, ErrorClassKey.APPLICATION_FAILURE).value()).isEqualTo(3d);
        assertThat(rateOf(errors, ErrorClassKey.CLIENT_INVALID).value()).isEqualTo(6d);
        assertThat(rateOf(errors, ErrorClassKey.POLICY_REJECT).value()).isEqualTo(10d);
        assertThat(rateOf(errors, ErrorClassKey.DEPENDENCY_FAILURE).state())
                .isEqualTo(SourceStatus.VALID);
        assertThat(errors.classes()).allSatisfy(errorClass ->
                assertThat(errorClass.rate().value()).isBetween(0d, 100d));
        // 관측 시각은 평가 시각이 아니라 마지막 스크레이프 시각이다.
        assertThat(rateOf(errors, ErrorClassKey.APPLICATION_FAILURE).observedAt())
                .isEqualTo(EVALUATED.minusSeconds(FRESH_AGE_SECONDS));
    }

    /**
     * 정책 거절은 설계된 동작입니다. 분자에 넣으면 재고가 소진된 뒤 실패율이 100% 에 붙어
     * 경보가 아무 의미도 갖지 못합니다.
     */
    @Test
    @DisplayName("재고가 소진돼 정책 거절이 트래픽의 99% 여도 시스템 실패율은 0 이다")
    void policyRejectStaysOutOfTheNumerator() {
        FakePromQuery client = respond(Map.of(
                "rate(app_http", List.of(
                        rate("issue", "success", 10d, "api-1"),
                        rate("issue", "policy_reject", 990d, "api-1"),
                        // 실패 시계열은 기동 시점에 등록돼 트래픽이 없어도 0 으로 나온다.
                        rate("issue", "dependency_failure", 0d, "api-1"),
                        rate("issue", "application_failure", 0d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        ErrorMetrics errors = assemble(client, globalQuery()).errors();

        assertThat(rateOf(errors, ErrorClassKey.DEPENDENCY_FAILURE).value()).isEqualTo(0d);
        assertThat(rateOf(errors, ErrorClassKey.APPLICATION_FAILURE).value()).isEqualTo(0d);
        // 표에서 지우지는 않는다 — 안 보이면 그 트래픽이 어디로 갔는지 아무도 설명하지 못한다.
        assertThat(rateOf(errors, ErrorClassKey.POLICY_REJECT).value()).isEqualTo(99d);
        assertThat(excluded(errors, ErrorClassKey.POLICY_REJECT)).isTrue();
        assertThat(excluded(errors, ErrorClassKey.CLIENT_INVALID)).isTrue();
        assertThat(excluded(errors, ErrorClassKey.DEPENDENCY_FAILURE)).isFalse();
        assertThat(excluded(errors, ErrorClassKey.APPLICATION_FAILURE)).isFalse();
    }

    /**
     * 대기열 수락은 {@code entry} 경로에서 나옵니다. uri 그룹을 안 걸면 순번 폴링이 분모를
     * 부풀려 실패율이 실제보다 작게 나옵니다.
     */
    @Test
    @DisplayName("대기열 경로는 분모에도 분자에도 섞이지 않는다")
    void queuePathDoesNotReachTheFailureRate() {
        FakePromQuery client = respond(Map.of(
                "rate(app_http", List.of(
                        rate("issue", "success", 90d, "api-1"),
                        rate("issue", "application_failure", 10d, "api-1"),
                        rate("entry", "queue_accepted", 900d, "api-1"),
                        rate("queue", "application_failure", 500d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        ErrorMetrics errors = assemble(client, globalQuery()).errors();

        // 분모가 1000 이었으면 1%, 분자에 queue 가 섞였으면 510% 가 나온다.
        assertThat(rateOf(errors, ErrorClassKey.APPLICATION_FAILURE).value()).isEqualTo(10d);
    }

    /** 요청이 0 건이면 나눌 것이 없습니다. 비율 0 이 아니라 정의되지 않는 값입니다. */
    @Test
    @DisplayName("발급 시도가 0 이면 비율은 0 이 아니라 N_A 다")
    void zeroAttemptsMakeTheRateUndefined() {
        FakePromQuery client = respond(Map.of(
                "rate(app_http", List.of(rate("issue", "success", 0d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        ErrorMetrics errors = assemble(client, globalQuery()).errors();

        assertThat(errors.classes()).allSatisfy(errorClass -> {
            assertThat(errorClass.rate().state()).isEqualTo(SourceStatus.N_A);
            assertThat(errorClass.rate().value()).isNull();
        });
    }

    /**
     * 네 분류는 서로 겹치지 않고 모두 분모 안에 들어 있습니다. 그래서 넷을 다 더해도 100 을
     * 넘을 수 없습니다 — 넘으면 필터가 그룹 밖을 집었거나 두 분류가 같은 표본을 세고 있는 것입니다.
     *
     * <p>자르는 코드를 두지 않기로 한 결정이 여기 걸려 있습니다. 누가 "안전하게"
     * {@code Math.min(percent, 100)} 을 넣으면 그 배선 오류가 화면에서 정상으로 보이게 됩니다 —
     * 그때 이 테스트가 아니라 <b>합이 100 을 넘는 사실 자체</b>가 먼저 드러나야 합니다.</p>
     */
    @Test
    @DisplayName("겹치지 않는 네 분류의 비율 합은 100 을 넘지 않는다")
    void disjointClassRatesNeverExceedTheWhole() {
        FakePromQuery client = respond(Map.of(
                "rate(app_http", List.of(
                        rate("issue", "success", 500d, "api-1"),
                        rate("issue", "dependency_failure", 100d, "api-1"),
                        rate("issue", "application_failure", 150d, "api-1"),
                        rate("issue", "client_invalid", 50d, "api-1"),
                        rate("issue", "policy_reject", 200d, "api-1"),
                        // 발급 경로 밖이다. 분자가 이걸 집으면 합이 100 을 넘는다.
                        rate("queue", "application_failure", 900d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        ErrorMetrics errors = assemble(client, globalQuery()).errors();

        double sum = errors.classes().stream()
                .mapToDouble(errorClass -> errorClass.rate().value())
                .sum();
        assertThat(sum)
                .as("합이 100 을 넘으면 필터가 그룹 밖을 집었거나 두 분류가 같은 표본을 센 것이다")
                .isLessThanOrEqualTo(100d)
                .isEqualTo(50d);
    }

    /**
     * counter 의 rate 는 음수가 될 수 없습니다. 음수가 왔다면 원천이 망가진 것이라 비율을 낼 수
     * 없습니다 — 그대로 나누면 <b>음수 백분율이 VALID 로 나가</b> 화면이 그것을 실패율로 그립니다.
     *
     * <p>이 경로는 표본만으로 도달합니다. 분모가 음수면 분자가 분모를 넘어 0~100 계약이 깨집니다.</p>
     */
    @Test
    @DisplayName("음수 표본이 오면 처리량도 실패율도 UNAVAILABLE 로 내려보낸다")
    void negativeSamplesCannotProduceARate() {
        FakePromQuery client = respond(Map.of(
                "rate(app_http", List.of(
                        // counter 가 gauge 로 잘못 등록되는 등 원천이 망가진 상태다.
                        rate("issue", "success", -100d, "api-1"),
                        rate("issue", "application_failure", 50d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        ObservedValue<Double> rate =
                rateOf(response.errors(), ErrorClassKey.APPLICATION_FAILURE);
        assertThat(rate.state())
                .as("-100% 가 VALID 로 나가면 화면이 그것을 실패율로 그린다")
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(rate.value()).isNull();

        // 처리량과 실패율은 같은 값을 나눠 쓴다. 한쪽만 막으면 같은 스냅샷에서 처리량은 음수 값을
        // VALID 로 그리고 실패율만 비는 모순이 생긴다.
        assertThat(response.traffic().issueAttemptRps().state())
                .as("같은 표본을 두 패널이 다르게 판정하면 화면이 스스로 모순된다")
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /**
     * 브라우저·부하 생성기 쪽 실패는 서버가 볼 수 있는 원천이 아예 없습니다. 0 으로 실으면
     * "클라이언트 실패 없음" 이라는 거짓 신호가 되므로 키 자체를 내보내지 않습니다.
     */
    @Test
    @DisplayName("원천이 없는 clientObservedFailure 는 키 자체가 응답에 없다")
    void clientObservedFailureHasNoKey() {
        ErrorMetrics errors = assemble(FakePromQuery.empty(), globalQuery()).errors();

        assertThat(errors.classes()).extracting(errorClass -> errorClass.key().jsonValue())
                .containsExactly("dependencyFailure", "applicationFailure", "clientInvalid",
                        "policyReject")
                .doesNotContain("clientObservedFailure");
    }

    /**
     * 원인 표의 원천은 HTTP 결과가 아니라 발급 결과 사유입니다. 0 인 행도 그대로 내려보냅니다 —
     * 무엇이 0 인지 보이는 편이 나은지는 패널이 정할 문제입니다.
     */
    @Test
    @DisplayName("실패 원인은 사유별 초당 건수를 큰 순으로 담고 0 행도 남긴다")
    void topReasonsCarryFailureReasonRates() {
        FakePromQuery client = respond(Map.of(
                "app_issuance_outcome_total", List.of(
                        outcome(ReasonCode.INTERNAL_ERROR, 2.5d),
                        outcome(ReasonCode.TEMPORARILY_UNAVAILABLE, 7.25d),
                        outcome(ReasonCode.UNMAPPED, 0d)),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        var topReasons = assemble(client, globalQuery()).errors().topReasons();

        assertThat(topReasons.state()).isEqualTo(SourceStatus.VALID);
        assertThat(topReasons.value()).extracting(TopReason::reasonCode)
                .containsExactly(ReasonCode.TEMPORARILY_UNAVAILABLE, ReasonCode.INTERNAL_ERROR,
                        ReasonCode.UNMAPPED);
        assertThat(topReasons.value().get(0).rps()).isEqualTo(7.25d);
        assertThat(topReasons.value().get(2).rps()).isEqualTo(0d);
    }

    /**
     * 실패율과 같은 판정입니다. 사유 하나가 음수면 원천이 망가진 것이고, 그 행만 빼면 남은
     * 행의 순위가 멀쩡한 것처럼 보입니다.
     */
    @Test
    @DisplayName("음수 사유가 섞이면 원인 표를 통째로 비운다")
    void negativeReasonRateEmptiesTheWholeTable() {
        FakePromQuery client = respond(Map.of(
                "app_issuance_outcome_total", List.of(
                        outcome(ReasonCode.INTERNAL_ERROR, 5d),
                        outcome(ReasonCode.TEMPORARILY_UNAVAILABLE, -1d)),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        var topReasons = assemble(client, globalQuery()).errors().topReasons();

        assertThat(topReasons.state())
                .as("음수 행만 빼면 남은 순위가 멀쩡한 것처럼 보인다")
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(topReasons.value()).isNull();
    }

    /**
     * "실패가 없어서 빈 표" 와 "아직 못 물어봐서 빈 표" 는 운영자가 취할 행동이 정반대입니다.
     * 시계열이 하나도 없으면 빈 목록이 아니라 PENDING 입니다.
     */
    @Test
    @DisplayName("원인 시계열이 하나도 없으면 빈 목록이 아니라 PENDING 이다")
    void missingReasonSeriesIsPendingNotEmpty() {
        FakePromQuery client = respond(Map.of(
                "rate(app_http", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        var topReasons = assemble(client, globalQuery()).errors().topReasons();

        assertThat(topReasons.state()).isEqualTo(SourceStatus.PENDING);
        assertThat(topReasons.value()).isNull();
    }

    /** 상태는 블록이 아니라 값마다 붙습니다. 한 계열이 죽어도 나머지는 살아야 합니다. */
    @Test
    @DisplayName("원인 질의만 죽어도 분류 표는 산다")
    void deadReasonQueryLeavesTheClassTableAlive() {
        FakePromQuery client = new FakePromQuery(promQl -> {
            if (promQl.contains("app_issuance_outcome_total")) {
                throw new PromQueryException("시험용 장애: " + promQl);
            }
            if (promQl.startsWith("rate(app_http")) {
                return List.of(rate("issue", "success", 90d, "api-1"),
                        rate("issue", "application_failure", 10d, "api-1"));
            }
            if (promQl.contains("timestamp(")) {
                return List.of(age(FRESH_AGE_SECONDS));
            }
            return List.of();
        });

        ErrorMetrics errors = assemble(client, globalQuery()).errors();

        assertThat(errors.topReasons().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(rateOf(errors, ErrorClassKey.APPLICATION_FAILURE).value()).isEqualTo(10d);
        assertThat(rateOf(errors, ErrorClassKey.APPLICATION_FAILURE).state())
                .isEqualTo(SourceStatus.VALID);
    }

    /** 스크레이프가 멈추면 분류 표도 원인 표도 값을 싣되 STALE 로 나갑니다. */
    @Test
    @DisplayName("마지막 스크레이프가 오래되면 실패율과 원인 표가 STALE 로 나간다")
    void oldScrapeMakesErrorsStale() {
        FakePromQuery client = respond(Map.of(
                "rate(app_http", List.of(
                        rate("issue", "success", 90d, "api-1"),
                        rate("issue", "application_failure", 10d, "api-1")),
                "app_issuance_outcome_total", List.of(outcome(ReasonCode.INTERNAL_ERROR, 1d)),
                "timestamp(", List.of(age(STALE_AGE_SECONDS))));

        ErrorMetrics errors = assemble(client, globalQuery()).errors();

        assertThat(rateOf(errors, ErrorClassKey.APPLICATION_FAILURE).state())
                .isEqualTo(SourceStatus.STALE);
        assertThat(errors.topReasons().state()).isEqualTo(SourceStatus.STALE);
    }

    /** 예산이 모자라면 원인 질의부터 잘립니다 — 마지막에 보내는 질의라 그것만 비어야 합니다. */
    @Test
    @DisplayName("실패 원인 질의는 마지막에 나간다")
    void reasonQueryGoesLast() {
        FakePromQuery client = FakePromQuery.empty();
        assemble(client, globalQuery());

        assertThat(client.queries().get(client.queries().size() - 1))
                .contains("app_issuance_outcome_total");
    }

    // ── 지연 ────────────────────────────────────────────────────────────────────

    /** 인스턴스별 백분위는 병합할 수 없습니다. 최댓값을 씁니다(DEC-02). */
    @Test
    @DisplayName("백분위는 인스턴스 최댓값을 쓰고 ms 로 환산한다")
    void percentilesTakeInstanceMax() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile("0.5", 0.010d, "api-1"),
                        quantile("0.5", 0.020d, "api-2"),
                        quantile("0.95", 0.100d, "api-1"),
                        quantile("0.99", 0.250d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        var success = assemble(client, globalQuery()).latency().success();

        assertThat(success.state()).isEqualTo(SourceStatus.VALID);
        assertThat(success.value().p50Millis()).isEqualTo(20d);
        assertThat(success.value().p95Millis()).isEqualTo(100d);
        assertThat(success.value().p99Millis()).isEqualTo(250d);
    }

    /** expiry 로 창이 비면 백분위가 0 으로 읽힙니다. 그대로 내보내면 "지연 0ms" 를 그립니다. */
    @Test
    @DisplayName("관측 창이 비어 백분위가 0이면 0ms 가 아니라 PENDING 이다")
    void zeroPercentileIsPendingNotZeroLatency() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile("0.5", 0d, "api-1"),
                        quantile("0.95", 0d, "api-1"),
                        quantile("0.99", 0d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        var success = assemble(client, globalQuery()).latency().success();

        assertThat(success.state()).isEqualTo(SourceStatus.PENDING);
        assertThat(success.value()).isNull();
    }

    /**
     * 라벨 표기는 계측과 응답 두 파일에 걸친 계약입니다. 한쪽만 검증하면 갈라진 것을 못 잡습니다 —
     * 여기서 {@code UriGroup} enum 을 정본으로 두고 응답 라벨 전체와 맞춥니다.
     */
    @Test
    @DisplayName("groups 는 UriGroup 다섯 종을 enum 순서 그대로, 미터 라벨 표기로 낸다")
    void groupsCoverEveryUriGroupWithMeterLabels() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(quantile("0.99", 0.250d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        List<LatencyGroupStat> groups = assemble(client, globalQuery()).latency().groups();

        assertThat(groups).extracting(LatencyGroupStat::group)
                .containsExactly(Arrays.stream(UriGroup.values())
                        .map(UriGroup::tagValue).toArray(String[]::new));
        assertThat(groups).extracting(LatencyGroupStat::group)
                .allMatch(label -> label.equals(label.toLowerCase(Locale.ROOT)));
    }

    /** 그룹을 편 것이지 대체한 것이 아닙니다. 기존 세 필드는 그대로 있어야 합니다. */
    @Test
    @DisplayName("groups 를 더해도 success·policyReject·systemFailure 는 그대로다")
    void existingThreeLatencyFieldsSurvive() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile("0.5", 0.010d, "api-1"),
                        quantile("0.95", 0.100d, "api-1"),
                        quantile("0.99", 0.250d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        LatencyMetrics latency = assemble(client, globalQuery()).latency();

        assertThat(latency.success().state()).isEqualTo(SourceStatus.VALID);
        assertThat(latency.success().value().p99Millis()).isEqualTo(250d);
        // [OBS-31] 원천은 생겼지만 이 픽스처는 outcome=success 표본만 준다. 실패 축에 표본이
        // 없을 때 PENDING 인 것은 지금도 맞다 — 라벨로 값을 만드는 경로는
        // HttpLatencyOutcomeContractTest 가 계측과 함께 본다.
        assertThat(latency.policyReject().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(latency.systemFailure().state()).isEqualTo(SourceStatus.PENDING);
    }

    /**
     * 같은 원천·같은 필터라 두 자리가 갈리면 화면이 같은 수치를 다르게 읽습니다. 조립기가
     * 헬퍼 하나를 나눠 쓰는지를 값으로 확인합니다.
     */
    @Test
    @DisplayName("groups 의 issue 항목과 latency.success 는 언제나 같다")
    void issueGroupEqualsSuccessField() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile("0.5", 0.010d, "api-1"),
                        quantile("0.95", 0.100d, "api-1"),
                        quantile("0.99", 0.250d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        LatencyMetrics latency = assemble(client, globalQuery()).latency();

        assertThat(group(latency, UriGroup.ISSUE)).isEqualTo(latency.success());
    }

    /** 부하 회차에 따라 READ·USE 는 표본이 0 일 수 있습니다. 목록에서 빼면 이유가 사라집니다. */
    @Test
    @DisplayName("표본 없는 그룹도 목록에 남고 빈 값이 아니라 상태로 온다")
    void groupWithoutSamplesStaysWithState() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile("0.5", 0.010d, "api-1"),
                        quantile("0.95", 0.100d, "api-1"),
                        quantile("0.99", 0.250d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        LatencyMetrics latency = assemble(client, globalQuery()).latency();

        assertThat(group(latency, UriGroup.READ).state()).isEqualTo(SourceStatus.PENDING);
        assertThat(group(latency, UriGroup.READ).value()).isNull();
        assertThat(group(latency, UriGroup.READ).observedAt()).isNull();
    }

    /** 그룹마다 최댓값을 낸 인스턴스가 다를 수 있습니다. 그룹 안에서만 접혀야 합니다. */
    @Test
    @DisplayName("그룹별 백분위는 그 그룹 안의 인스턴스 최댓값이다")
    void eachGroupFoldsWithinItself() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile(UriGroup.ISSUE, "0.5", 0.010d, "api-1"),
                        quantile(UriGroup.ISSUE, "0.95", 0.100d, "api-1"),
                        quantile(UriGroup.ISSUE, "0.99", 0.250d, "api-1"),
                        quantile(UriGroup.READ, "0.5", 0.001d, "api-1"),
                        quantile(UriGroup.READ, "0.5", 0.002d, "api-2"),
                        quantile(UriGroup.READ, "0.95", 0.004d, "api-2"),
                        quantile(UriGroup.READ, "0.99", 0.008d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        LatencyMetrics latency = assemble(client, globalQuery()).latency();

        LatencyPercentiles read = group(latency, UriGroup.READ).value();
        assertThat(read.p50Millis()).isEqualTo(2d);
        assertThat(read.p95Millis()).isEqualTo(4d);
        assertThat(read.p99Millis()).isEqualTo(8d);
        // 다른 그룹의 값이 섞이지 않는다.
        assertThat(group(latency, UriGroup.ISSUE).value().p99Millis()).isEqualTo(250d);
    }

    /**
     * 원천이 instance 라벨을 떨구면(federation · recording rule) 라벨 없는 표본끼리 짝이 맞아
     * 어느 대의 것도 아닌 지연이 VALID 로 나갑니다. 값이 정상 범위라 예외도 로그도 안 남습니다 —
     * CY-449 가 사용률에서 내린 것과 같은 판단으로 값을 내지 않습니다.
     */
    @Test
    @DisplayName("instance 라벨 없는 표본에서는 값을 내지 않는다")
    void samplesWithoutInstanceLabelYieldNoValue() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile("0.5", 0.010d, ""),
                        quantile("0.95", 0.100d, ""),
                        quantile("0.99", 0.250d, "")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        LatencyMetrics latency = assemble(client, globalQuery()).latency();

        assertThat(latency.success().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(latency.success().value()).isNull();
        assertThat(group(latency, UriGroup.ISSUE).state()).isEqualTo(SourceStatus.PENDING);
    }

    // ── 지연: 트래픽 없음과 표본 없음 (OBS-44) ─────────────────────────────────

    /**
     * 요청이 0 건이면 백분위는 <b>정의되지 않습니다</b>. PENDING 은 "곧 값이 온다" 로 읽히므로
     * 화면이 오지 않을 값을 기다립니다 — {@code N_A} 여야 합니다.
     *
     * <p>{@code NO_TRAFFIC} 이 아닌 이유는 그쪽이 값을 요구하는데({@code carriesValue()==true})
     * 실을 값이 (0,0,0) 뿐이고, 그 0 은 "지연 0ms" 라는 더 나쁜 거짓말이기 때문입니다.</p>
     */
    @Test
    @DisplayName("트래픽이 0 이면 지연 백분위는 PENDING 이 아니라 N_A 다")
    void zeroTrafficMakesLatencyNotApplicable() {
        FakePromQuery client = respond(Map.of(
                "rate(", zeroRates(),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        LatencyMetrics latency = assemble(client, globalQuery()).latency();

        assertThat(latency.success().state()).isEqualTo(SourceStatus.N_A);
        // N_A 는 값을 싣지 않는다. 실으려 하면 ObservedValue 생성자가 막는다.
        assertThat(latency.success().value()).isNull();
        assertThat(latency.success().observedAt()).isNull();
    }

    /**
     * 한쪽만 고치면 같은 화면의 두 자리가 다른 규칙으로 상태를 냅니다. 전역 3필드와 groups
     * 5종이 {@code percentiles()} 헬퍼 하나에서 갈리는지를 값으로 확인합니다.
     */
    @Test
    @DisplayName("트래픽 0 에서 전역 3필드와 groups 5종이 모두 같은 상태로 온다")
    void zeroTrafficAppliesToEveryLatencyField() {
        FakePromQuery client = respond(Map.of(
                "rate(", zeroRates(),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        LatencyMetrics latency = assemble(client, globalQuery()).latency();

        assertThat(latency.success().state()).isEqualTo(SourceStatus.N_A);
        assertThat(latency.policyReject().state()).isEqualTo(SourceStatus.N_A);
        assertThat(latency.systemFailure().state()).isEqualTo(SourceStatus.N_A);
        assertThat(latency.groups()).hasSize(UriGroup.values().length);
        assertThat(latency.groups()).allSatisfy(stat ->
                assertThat(stat.percentiles().state()).isEqualTo(SourceStatus.N_A));
    }

    /**
     * 트래픽이 있는데 백분위 표본이 없는 것은 <b>다른 사건</b>입니다 — Micrometer expiry(10s)
     * 로 관측 창이 비었다는 뜻이라 다음 폴링에 값이 올 수 있습니다. 이것까지 N_A 로 접으면
     * 이 티켓이 가르려던 두 사건이 다시 하나가 됩니다.
     */
    @Test
    @DisplayName("트래픽은 있는데 백분위 표본이 없으면 PENDING 이다")
    void trafficWithoutSamplesStaysPending() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        LatencyMetrics latency = assemble(client, globalQuery()).latency();

        assertThat(latency.success().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(group(latency, UriGroup.ISSUE).state()).isEqualTo(SourceStatus.PENDING);
    }

    /**
     * {@code percentiles()} 는 값 없이 두 곳으로 빠집니다 — 시계열이 아예 없거나, expiry 로
     * 창이 비어 p99 가 0 으로 읽히거나. <b>둘은 같은 사건의 다른 얼굴</b>이라 같은 규칙을
     * 써야 합니다. 출구 하나만 고치면 트래픽 0 이 표본 유무에 따라 두 상태로 갈립니다.
     */
    @Test
    @DisplayName("p99 가 0 인 출구도 트래픽 0 이면 N_A, 트래픽이 있으면 PENDING 이다")
    void zeroPercentileExitFollowsTheSameRule() {
        List<PromSample> emptyWindow = List.of(
                quantile("0.5", 0d, "api-1"),
                quantile("0.95", 0d, "api-1"),
                quantile("0.99", 0d, "api-1"));

        FakePromQuery noTraffic = respond(Map.of(
                "quantile!=", emptyWindow,
                "rate(", zeroRates(),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));
        assertThat(assemble(noTraffic, globalQuery()).latency().success().state())
                .isEqualTo(SourceStatus.N_A);

        FakePromQuery expired = respond(Map.of(
                "quantile!=", emptyWindow,
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));
        assertThat(assemble(expired, globalQuery()).latency().success().state())
                .isEqualTo(SourceStatus.PENDING);
    }

    /**
     * 분모는 그룹 전체가 아니라 <b>그 축으로 접히는 결과 분류</b>입니다
     * ({@code LatencyOutcome.of}). 그룹 전체를 쓰면 트래픽이 전부 성공인 동안 실패 두 축이
     * "기다릴 표본이 없는데 기다리라" 는 PENDING 으로 굳습니다.
     */
    @Test
    @DisplayName("성공만 흐르면 성공 축은 값을 내고 실패 두 축은 N_A 다")
    void eachOutcomeAxisUsesItsOwnDenominator() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile("0.5", 0.010d, "api-1"),
                        quantile("0.95", 0.100d, "api-1"),
                        quantile("0.99", 0.250d, "api-1")),
                "rate(", List.of(
                        rate("issue", "success", 5000d, "api-1"),
                        rate("issue", "policy_reject", 0d, "api-1"),
                        rate("issue", "client_invalid", 0d, "api-1"),
                        rate("issue", "dependency_failure", 0d, "api-1"),
                        rate("issue", "application_failure", 0d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        LatencyMetrics latency = assemble(client, globalQuery()).latency();

        assertThat(latency.success().state()).isEqualTo(SourceStatus.VALID);
        assertThat(latency.success().value().p99Millis()).isEqualTo(250d);
        assertThat(latency.policyReject().state()).isEqualTo(SourceStatus.N_A);
        assertThat(latency.systemFailure().state()).isEqualTo(SourceStatus.N_A);
    }

    /**
     * 접는 규칙은 {@code LatencyOutcome.of} 하나입니다. {@code dependency_failure} 하나만
     * 흘러도 시스템 실패 축의 분모는 0 이 아니어야 합니다 — 조립기가 규칙을 옮겨 적었으면
     * 여기서 갈립니다.
     */
    @Test
    @DisplayName("시스템 실패 축의 분모는 systemFailures() 전체를 접는다")
    void systemFailureDenominatorFoldsEveryFailureClass() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile(UriGroup.ISSUE, "system_failure", "0.5", 0.100d, "api-1"),
                        quantile(UriGroup.ISSUE, "system_failure", "0.95", 0.200d, "api-1"),
                        quantile(UriGroup.ISSUE, "system_failure", "0.99", 3d, "api-1")),
                "rate(", List.of(
                        rate("issue", "success", 0d, "api-1"),
                        rate("issue", "policy_reject", 0d, "api-1"),
                        rate("issue", "client_invalid", 0d, "api-1"),
                        rate("issue", "dependency_failure", 10d, "api-1"),
                        rate("issue", "application_failure", 0d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        LatencyMetrics latency = assemble(client, globalQuery()).latency();

        assertThat(latency.systemFailure().state()).isEqualTo(SourceStatus.VALID);
        assertThat(latency.systemFailure().value().p99Millis()).isEqualTo(3000d);
        // 성공 축은 같은 스냅샷에서 트래픽이 0 이라 N_A 다. 두 축이 서로 섞이지 않는다.
        assertThat(latency.success().state()).isEqualTo(SourceStatus.N_A);
    }

    /**
     * <b>분모를 못 읽었으면 단정하지 않습니다.</b> rate 를 못 읽었을 뿐 트래픽이 있었을 수
     * 있고, "트래픽 없음" 을 단정하면 이 티켓이 없애려는 거짓말을 다른 자리에서 다시 만듭니다.
     * PENDING 은 정보가 아니라 정직함입니다.
     *
     * <p>지연 <b>값</b> 자체는 살아 있어야 합니다 — 백분위는 지연 질의에서 나오므로 결과
     * 질의가 죽어도 그리는 데 아무것도 모자라지 않습니다.</p>
     */
    @Test
    @DisplayName("결과 질의가 죽으면 지연은 값을 그대로 내고, 값이 없을 때만 PENDING 으로 물러난다")
    void unreadableDenominatorFallsBackToPending() {
        List<PromSample> latencySamples = List.of(
                quantile("0.5", 0.010d, "api-1"),
                quantile("0.95", 0.100d, "api-1"),
                quantile("0.99", 0.250d, "api-1"));
        FakePromQuery client = new FakePromQuery(promQl -> {
            if (promQl.startsWith("rate(" + MetricAggregation.HTTP_RESULT_TOTAL)) {
                throw new PromQueryException("시험용 결과 질의 장애");
            }
            if (promQl.contains("quantile!=")) {
                return latencySamples;
            }
            if (promQl.contains("timestamp(")) {
                return List.of(age(FRESH_AGE_SECONDS));
            }
            return List.of();
        });

        AdminMetricsResponse response = assemble(client, globalQuery());
        LatencyMetrics latency = response.latency();

        // 분모가 죽은 것이 지연 값을 죽이지 않는다.
        assertThat(response.traffic().issueAttemptRps().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(latency.success().state()).isEqualTo(SourceStatus.VALID);
        assertThat(latency.success().value().p99Millis()).isEqualTo(250d);
        // 표본이 없는 축은 N_A 로 단정하지 않고 PENDING 으로 물러난다.
        assertThat(latency.policyReject().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(latency.systemFailure().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(group(latency, UriGroup.READ).state()).isEqualTo(SourceStatus.PENDING);
    }

    /**
     * <b>인스턴스 하나가 스크레이프에서 빠지면 분모는 살아 있는 쪽만 합산해 0 이 됩니다.</b>
     * 나이는 {@code max} 로 재므로(빠진 쪽이 드러나야 합니다) 신선도만 오래됩니다. 그 0 을
     * 트래픽 없음으로 읽으면 빠진 인스턴스가 처리하던 요청이 "요청 0 건" 으로 그려집니다.
     *
     * <p>같은 스냅샷의 처리량 칸이 STALE 이라는 것이 근거입니다 — 두 칸이 같은 표본을 보는데
     * 한쪽만 "없다" 를 단정하면 화면에 모순이 남습니다.</p>
     */
    @Test
    @DisplayName("원천이 STALE 이면 rate 0 을 트래픽 없음으로 읽지 않고 PENDING 이다")
    void staleSourceNeverClaimsZeroTraffic() {
        FakePromQuery client = respond(Map.of(
                "rate(", zeroRates(),
                "timestamp(", List.of(age(STALE_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());
        LatencyMetrics latency = response.latency();

        // 처리량 칸은 STALE 로 남는다. 그쪽은 이미 "믿지 마라" 를 싣고 있어 값을 거둘 이유가
        // 없다 — 지연 축만 값 없이 낼 수 있어서 PENDING 으로 물러난다.
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.STALE);
        assertThat(latency.success().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(latency.policyReject().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(latency.systemFailure().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(latency.groups()).allSatisfy(stat ->
                assertThat(stat.percentiles().state()).isEqualTo(SourceStatus.PENDING));
    }

    /**
     * <b>STALE 검사만으로는 부족합니다.</b> {@code staleAfter}(120초)가 집계 창(1분)보다 길어,
     * 빠진 인스턴스의 나이가 60~120초인 구간은 창을 이미 넘겼는데도 신선도가 정상으로 나옵니다.
     *
     * <p>이 테스트가 {@code staleAfter} 와 창 길이의 관계를 고정합니다 — 둘 중 하나를 만지면
     * 여기서 갈립니다.</p>
     */
    @Test
    @DisplayName("관측 시각이 집계 창 밖이면 아직 STALE 이 아니어도 트래픽 0 을 단정하지 않는다")
    void observationOlderThanTheWindowNeverClaimsZeroTraffic() {
        long beyondWindowButFresh = MetricsWindow.ONE_MINUTE.seconds() + 30;
        assertThat(beyondWindowButFresh).isLessThan(STALE_AFTER.toSeconds());

        FakePromQuery client = respond(Map.of(
                "rate(", zeroRates(),
                "timestamp(", List.of(age(beyondWindowButFresh))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        // 신선도가 정상이라 STALE 도 안 붙는 구간이다. 여기서 처리량 칸만 "요청 0 건" 을
        // 단정하면 한 화면에서 지연은 "모름", 처리량은 "0 건 확정" 이 되어 운영자가 어느 쪽을
        // 믿을지 판단할 근거가 없다 — 두 칸이 같은 표본을 보므로 같은 판정을 써야 한다.
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.traffic().issueAttemptRps().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.latency().success().state()).isEqualTo(SourceStatus.PENDING);
    }

    /**
     * 신선도 질의만 예산에 잘리는 것은 실제로 일어납니다 — 그 질의가 네 번째라 예산
     * (기본 500ms)이 모자라면 결과 질의는 살고 이것부터 잘립니다. 관측 시각을 모르면 창이
     * 관측 뒤인지 판단할 근거가 없으므로 트래픽 0 을 단정할 수 없습니다.
     */
    @Test
    @DisplayName("관측 시각을 모르면 rate 0 이어도 N_A 로 단정하지 않는다")
    void unknownObservationTimeNeverClaimsZeroTraffic() {
        // 신선도 표본만 없다. 결과 rate 는 정상으로 0 이 온다.
        FakePromQuery client = respond(Map.of("rate(", zeroRates()));

        assertThat(assemble(client, globalQuery()).latency().success().state())
                .isEqualTo(SourceStatus.PENDING);
    }

    /**
     * <b>STALE 인 0 이 실패율에서 N_A 로 접히면 낡았다는 사실이 통째로 지워집니다.</b> N_A 는
     * 값도 시각도 싣지 않아, 화면에는 "요청 0 건이라 비율이 정의되지 않음" 만 남고 원천이
     * 낡았다는 신호가 사라집니다. 부하가 한 인스턴스에 몰려 있고 그 인스턴스가 스크레이프에서
     * 빠지면 정확히 이 조합이 나옵니다.
     */
    @Test
    @DisplayName("분모가 STALE 인 0 이면 실패율은 N_A 가 아니라 PENDING 이다")
    void staleZeroDenominatorDoesNotMakeTheRateUndefined() {
        FakePromQuery client = respond(Map.of(
                "rate(", zeroRates(),
                "timestamp(", List.of(age(STALE_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        // 분모는 낡았다는 사실을 값과 함께 그대로 싣는다.
        assertThat(response.traffic().issueAttemptRps().state()).isEqualTo(SourceStatus.STALE);
        assertThat(response.errors().classes()).allSatisfy(errorClass ->
                assertThat(errorClass.rate().state()).isEqualTo(SourceStatus.PENDING));
    }

    /**
     * 실패 원인 표도 같은 scrape 에서 나옵니다 — 나이가 같으면 믿을 수 있는지도 같아야 합니다.
     * 실패율 칸이 PENDING 인 동안 바로 옆 원인 표가 전 사유 0 을 VALID 로 그리면, 운영자는
     * 그 표를 보고 "실패 없음" 으로 판단합니다.
     */
    @Test
    @DisplayName("분모를 믿을 수 없으면 전 행이 0 인 원인 표는 표째로 PENDING 이다")
    void untrustedAllZeroReasonTableFallsBackToPending() {
        long beyondWindowButFresh = MetricsWindow.ONE_MINUTE.seconds() + 30;
        FakePromQuery client = respond(Map.of(
                "rate(", zeroRates(),
                "sum by (outcome)", List.of(
                        outcome(ReasonCode.INTERNAL_ERROR, 0d),
                        outcome(ReasonCode.TEMPORARILY_UNAVAILABLE, 0d)),
                "timestamp(", List.of(age(beyondWindowButFresh))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.errors().topReasons().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.errors().topReasons().value()).isNull();
    }

    /**
     * 거두는 것은 "전부 0" 이라는 단정 하나뿐입니다. 한 사유라도 값이 있으면 표는 그대로
     * 나가야 합니다 — 그 표는 부분 탈락이라도 실제로 일어난 실패를 싣고 있습니다.
     */
    @Test
    @DisplayName("한 사유라도 값이 있으면 믿을 수 없는 구간에서도 원인 표는 그대로 나간다")
    void untrustedReasonTableWithAnyValueStillReports() {
        long beyondWindowButFresh = MetricsWindow.ONE_MINUTE.seconds() + 30;
        FakePromQuery client = respond(Map.of(
                "rate(", zeroRates(),
                "sum by (outcome)", List.of(
                        outcome(ReasonCode.INTERNAL_ERROR, 2d),
                        outcome(ReasonCode.TEMPORARILY_UNAVAILABLE, 0d)),
                "timestamp(", List.of(age(beyondWindowButFresh))));

        ObservedValue<List<TopReason>> topReasons =
                assemble(client, globalQuery()).errors().topReasons();

        assertThat(topReasons.state()).isEqualTo(SourceStatus.VALID);
        assertThat(topReasons.value()).hasSize(2);
    }

    // ── 정합성 ──────────────────────────────────────────────────────────────────

    /** 값 미터와 상태 미터는 짝입니다. 상태가 값을 요구할 때만 값을 싣습니다. */
    @Test
    @DisplayName("정합성 gap 은 값 미터와 상태 미터를 짝으로 읽는다")
    void consistencyPairsValueAndState() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        gap("active_db", Double.NaN),
                        gapState("active_db", SourceStatus.N_A),
                        gap("persist", Double.NaN),
                        gapState("persist", SourceStatus.UNAVAILABLE),
                        domain(MetricAggregation.CONSISTENCY_SEVERITY,
                                live(), SourceStatusCode.of(Severity.WARN)),
                        domain(MetricAggregation.CONSISTENCY_SEVERITY_STATE,
                                live(), SourceStatusCode.of(SourceStatus.VALID)),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var consistency = assemble(client, globalQuery()).consistency();

        assertThat(consistency.luaGap().value()).isEqualTo(3L);
        assertThat(consistency.luaGap().state()).isEqualTo(SourceStatus.VALID);
        // 관측 시각은 batch 의 마지막 수집 성공 시각이다. 평가 시각(=지금)이 아니다.
        assertThat(consistency.luaGap().observedAt())
                .isEqualTo(EVALUATED.minusSeconds(FRESH_AGE_SECONDS));
        // batch 가 실어 보낸 이유는 그대로 나간다. N_A 도 UNAVAILABLE 도 0 이 아니다.
        assertThat(consistency.activeDbGap().state()).isEqualTo(SourceStatus.N_A);
        assertThat(consistency.persistGap().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        // 상태 미터가 아예 없는 gap 은 PENDING 이다.
        assertThat(consistency.dbCounterGap().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(consistency.severity()).isEqualTo(Severity.WARN);
    }

    /**
     * 상태 VALID + 값 NaN 은 scrape 중 한 틱 어긋난 것이지 장애가 아닙니다
     * ({@code DomainGaugeRegistrar} 가 남는 창으로 문서화한 상태).
     */
    @Test
    @DisplayName("상태는 값을 요구하는데 값이 NaN 이면 장애색이 아니라 표시 없음이다")
    void oneTickSkewIsPendingNotUnavailable() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", Double.NaN),
                        gapState("lua", SourceStatus.VALID),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var luaGap = assemble(client, globalQuery()).consistency().luaGap();

        assertThat(luaGap.state()).isEqualTo(SourceStatus.PENDING);
        assertThat(luaGap.value()).isNull();
    }

    /** batch 수집이 오래 멈추면 gap 값이 남아 있어도 실시간 값으로 읽히면 안 됩니다. */
    @Test
    @DisplayName("batch 수집 성공이 오래되면 gap 이 STALE 로 나간다")
    void oldCollectionIsStale() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        collectSuccess(STALE_AGE_SECONDS))));

        var luaGap = assemble(client, globalQuery()).consistency().luaGap();

        assertThat(luaGap.state()).isEqualTo(SourceStatus.STALE);
        assertThat(luaGap.value()).isEqualTo(3L);
    }

    /**
     * 원천이 하나여야 하는 지표에 표본이 둘이면 어느 쪽도 고를 수 없습니다. 그렇다고 응답 전체를
     * 500 으로 날리면 batch 를 두 대로 늘리거나 scrape 대상을 추가한 순간 관제가 통째로 죽습니다.
     */
    @Test
    @DisplayName("SINGLE 규칙이 깨져도 그 값만 UNAVAILABLE 이고 응답은 살아남는다")
    void brokenSingleRuleDoesNotKillTheResponse() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        domain(MetricAggregation.CONSISTENCY_GAP,
                                Map.of(DomainMeterNames.TAG_GAP_TYPE, "lua",
                                        DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE,
                                        "instance", "batch-2"), 9d),
                        collectSuccess(FRESH_AGE_SECONDS)),
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        // 무관한 지표는 멀쩡해야 한다.
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.VALID);
    }

    /** batch 가 다른 회차를 보고 있으면 그 값은 요청한 회차의 값이 아닙니다. */
    @Test
    @DisplayName("요청 쿠폰과 batch 가 관측 중인 쿠폰이 다르면 N_A 로 나간다")
    void mismatchedCouponScopeIsNotApplicable() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        domain(MetricAggregation.CONSISTENCY_COUPON_ID, Map.of(), 77),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var response = assemble(client, new MetricsQuery(MetricsWindow.ONE_MINUTE, 42L, null));

        assertThat(response.scope().couponId()).isEqualTo(42L);
        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.N_A);
        assertThat(response.consistency().luaGap().value()).isNull();
    }


    /**
     * FINAL 판정은 조용해진 뒤 검증 배치가 같은 미터 이름으로 냅니다. 라벨로 가르지 않으면
     * 읽는 쪽이 둘을 같은 것으로 읽습니다.
     */
    @Test
    @DisplayName("phase=live 가 아닌 표본은 정합성 값에 섞이지 않는다")
    void finalPhaseSamplesAreNotMixedIntoLive() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        domain(MetricAggregation.CONSISTENCY_GAP,
                                Map.of(DomainMeterNames.TAG_GAP_TYPE, "lua",
                                        DomainMeterNames.TAG_PHASE, "final"), 999d),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var luaGap = assemble(client, globalQuery()).consistency().luaGap();

        assertThat(luaGap.state()).isEqualTo(SourceStatus.VALID);
        assertThat(luaGap.value()).isEqualTo(3L);
    }

    /**
     * 신선도 미터에 표본이 둘이면 어느 batch 의 시각인지 못 고릅니다. 그렇다고 응답 전체를
     * 500 으로 날리면 batch 를 두 대로 늘리는 순간 관제가 통째로 죽습니다.
     */
    @Test
    @DisplayName("신선도 미터의 SINGLE 규칙이 깨져도 응답은 살아남는다")
    void brokenFreshnessRuleDoesNotKillTheResponse() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        collectSuccess(FRESH_AGE_SECONDS),
                        domain(MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH,
                                Map.of(DomainMeterNames.TAG_COLLECT_PATH,
                                        DomainMeterNames.PATH_CONSISTENCY, "instance", "batch-2"),
                                EVALUATED.minusSeconds(9).getEpochSecond())),
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        // 관측 시각을 모르므로 표시 없음이지만, 무관한 지표는 멀쩡하다.
        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.VALID);
    }

    /**
     * PENDING("아직 안 나옴, 기다려라")과 UNAVAILABLE("원천이 죽었다, 조치하라")은 운영자가
     * 취할 행동이 정반대입니다.
     */
    @Test
    @DisplayName("신선도 질의만 실패하면 PENDING 이 아니라 UNAVAILABLE 이다")
    void freshnessQueryFailureIsUnavailableNotPending() {
        FakePromQuery client = new FakePromQuery(promQl -> {
            if (promQl.contains("timestamp(")) {
                throw new PromQueryException("시험용 장애");
            }
            if (promQl.startsWith("rate(")) {
                return List.of(rate("issue", "success", 40d, "api-1"));
            }
            return List.of();
        });

        var success = assemble(client, globalQuery()).traffic().issueSuccessTps();

        assertThat(success.state()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 낡은 판정을 지금 판정처럼 내보내면 화면이 초록불을 그립니다. */
    @Test
    @DisplayName("severity 가 낡았으면 값을 내지 않는다")
    void staleSeverityIsNotReported() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        domain(MetricAggregation.CONSISTENCY_SEVERITY,
                                live(), SourceStatusCode.of(Severity.NONE)),
                        domain(MetricAggregation.CONSISTENCY_SEVERITY_STATE,
                                live(), SourceStatusCode.of(SourceStatus.VALID)),
                        collectSuccess(STALE_AGE_SECONDS))));

        assertThat(assemble(client, globalQuery()).consistency().severity()).isNull();
    }

    /** 무한대는 값이 아닙니다. Long.MAX_VALUE 로 응답에 실리면 KPI 가 그대로 거짓 알람이 됩니다. */
    @Test
    @DisplayName("무한대 표본은 값이 아니라 표시 없음으로 처리한다")
    void infiniteSampleIsNotAValue() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", Double.POSITIVE_INFINITY),
                        gapState("lua", SourceStatus.VALID),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var luaGap = assemble(client, globalQuery()).consistency().luaGap();

        assertThat(luaGap.value()).isNull();
        assertThat(luaGap.state()).isEqualTo(SourceStatus.PENDING);
    }

    /** 어느 회차를 본 값인지 모르면 그 회차의 값이라고 말할 수 없습니다. */
    @Test
    @DisplayName("COUPON 범위인데 관측 회차를 모르면 N_A 다")
    void unknownObservedCouponIsNotApplicable() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var luaGap = assemble(client, new MetricsQuery(MetricsWindow.ONE_MINUTE, 42L, null))
                .consistency().luaGap();

        assertThat(luaGap.state()).isEqualTo(SourceStatus.N_A);
    }

    // ── 자원 포화 ───────────────────────────────────────────────────────────────

    /**
     * 행은 원천이 없어도 사라지지 않습니다. 지우면 화면이 "그 자원은 재 봤는데 여유" 로 읽습니다.
     * 이름·경고선·임계는 프론트 mock 과 같은 값이어야 색과 숫자가 따로 놀지 않습니다.
     */
    @Test
    @DisplayName("자원 6행과 임계가 화면 계약대로 나간다")
    void saturationCarriesSixContractRows() {
        AdminMetricsResponse response = assemble(FakePromQuery.empty(), globalQuery());

        assertThat(response.saturation().resources())
                .extracting(AdminMetricsResponse.ResourceRow::name)
                .containsExactly("Hikari", "Tomcat", "CPU", "Heap", "Redis", "디스크 · 네트워크");
        // DB 풀만 80 이다 — PRD 의 대기열 진입 조건과 같아야 한다.
        assertThat(response.saturation().resources().get(0).warnAt()).isEqualTo(80);
        assertThat(response.saturation().resources()).allSatisfy(row ->
                assertThat(row.warnAt()).isIn(75, 80));
        assertThat(response.saturation().thresholds().warn()).isEqualTo(60);
        assertThat(response.saturation().thresholds().high()).isEqualTo(75);
        assertThat(response.saturation().thresholds().critical()).isEqualTo(85);
    }

    /** 사용률의 분모가 없는 자원은 0 이 아니라 N_A 입니다. 0 은 "여유" 로 읽힙니다. */
    @Test
    @DisplayName("원천이 없는 자원 행은 N_A 로 나간다")
    void rowsWithoutSourceAreNotApplicable() {
        AdminMetricsResponse response = assemble(FakePromQuery.empty(), globalQuery());

        assertThat(response.saturation().resources().get(4).utilization().state())
                .isEqualTo(SourceStatus.N_A);
        assertThat(response.saturation().resources().get(5).utilization().state())
                .isEqualTo(SourceStatus.N_A);
    }

    /**
     * 인스턴스를 먼저 합치면 정원이 다른 대가 섞여 한 대의 포화가 다른 대의 여유에 희석됩니다.
     * 인스턴스 <b>안에서</b> 나눈 뒤 그 비율들을 최댓값으로 줄입니다.
     */
    @Test
    @DisplayName("사용률은 인스턴스 안에서 먼저 나누고 그 뒤 최댓값으로 줄인다")
    void utilizationFoldsInsideInstanceBeforeReducing() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(
                        pool(MetricAggregation.HIKARI_ACTIVE, 8d, "api-1", "HikariPool-1"),
                        pool(MetricAggregation.HIKARI_MAX, 10d, "api-1", "HikariPool-1"),
                        pool(MetricAggregation.HIKARI_ACTIVE, 5d, "api-2", "HikariPool-1"),
                        pool(MetricAggregation.HIKARI_MAX, 50d, "api-2", "HikariPool-1"))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        // 13/60 = 21.7% 가 아니라 max(80%, 10%) = 80% 다.
        assertThat(response.saturation().resources().get(0).utilization().value()).isEqualTo(80.0);
        assertThat(response.saturation().resources().get(0).utilization().state())
                .isEqualTo(SourceStatus.VALID);
    }

    /**
     * 관측 전용 풀은 발급 경로 자원이 아닙니다. 같이 세면 정원이 부풀어 사용률이 낮게 나옵니다 —
     * 값이 정상 범위라 아무도 못 알아챕니다.
     */
    @Test
    @DisplayName("DB 풀 사용률은 관측 전용 풀을 세지 않는다")
    void poolUtilizationExcludesObservationPool() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(
                        pool(MetricAggregation.HIKARI_ACTIVE, 8d, "api-1", "HikariPool-1"),
                        pool(MetricAggregation.HIKARI_MAX, 10d, "api-1", "HikariPool-1"),
                        pool(MetricAggregation.HIKARI_ACTIVE, 0d, "api-1", "obs-pool"),
                        pool(MetricAggregation.HIKARI_MAX, 2d, "api-1", "obs-pool"),
                        pool(MetricAggregation.HIKARI_PENDING, 7d, "api-1", "HikariPool-1"),
                        pool(MetricAggregation.HIKARI_PENDING, 3d, "api-1", "obs-pool"))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        // 관측 풀까지 세면 8/12 = 66.7% 가 된다.
        assertThat(response.saturation().resources().get(0).utilization().value()).isEqualTo(80.0);
        assertThat(response.saturation().resources().get(0).detail()).isEqualTo("pending 7");
    }

    /**
     * 이 미터는 {@code area}·{@code id} 로 여덟 갈래라 생표본에 최댓값을 걸면 heap 이 아니라
     * Metaspace 가 잡힙니다(실측). 그리고 G1 은 Eden·Survivor 상한에 -1 을 실어 영역 상한을
     * 더할 수 없습니다.
     */
    @Test
    @DisplayName("힙 사용률은 nonheap 영역과 -1 상한에 오염되지 않는다")
    void heapUtilizationIgnoresNonHeapAndUnknownLimits() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(
                        memory(MetricAggregation.JVM_MEMORY_USED, "heap", "G1 Eden Space", 30d),
                        memory(MetricAggregation.JVM_MEMORY_USED, "heap", "G1 Old Gen", 20d),
                        memory(MetricAggregation.JVM_MEMORY_USED, "nonheap", "Metaspace", 900d),
                        memory(MetricAggregation.JVM_MEMORY_MAX, "heap", "G1 Eden Space", -1d),
                        memory(MetricAggregation.JVM_MEMORY_MAX, "heap", "G1 Old Gen", 200d),
                        memory(MetricAggregation.JVM_MEMORY_MAX, "nonheap", "Metaspace", 1000d))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        // (30+20)/200 = 25%. nonheap 을 세면 475%, 상한을 더하면 25% 가 아니라 다른 값이 된다.
        assertThat(response.saturation().resources().get(3).utilization().value()).isEqualTo(25.0);
    }

    /**
     * 인스턴스 라벨이 없는 표본끼리 분자·분모가 맞아떨어지면 <b>어느 대의 것도 아닌 사용률</b>이
     * 나옵니다. 값이 정상 범위라 화면에서는 드러나지 않습니다.
     */
    @Test
    @DisplayName("인스턴스를 모르는 표본으로는 사용률을 내지 않는다")
    void samplesWithoutInstanceLabelYieldNoUtilization() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(
                        new PromSample(MetricAggregation.HIKARI_ACTIVE, Map.of("job", "api"), 8d, EVALUATED),
                        new PromSample(MetricAggregation.HIKARI_MAX, Map.of("job", "api"), 10d, EVALUATED))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.saturation().resources().get(0).utilization().state())
                .isEqualTo(SourceStatus.PENDING);
        assertThat(response.saturation().resources().get(0).utilization().value()).isNull();
    }

    /** 정원을 모르면 비율이 아닙니다. 0 으로 두면 그 자원이 가장 여유로워 보입니다. */
    @Test
    @DisplayName("정원 미터가 없으면 사용률은 0 이 아니라 PENDING 이다")
    void utilizationWithoutCapacityIsPending() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(
                        resource(MetricAggregation.TOMCAT_BUSY, 40d, "api-1"))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.saturation().resources().get(1).utilization().state())
                .isEqualTo(SourceStatus.PENDING);
        assertThat(response.saturation().resources().get(1).utilization().value()).isNull();
    }

    /** tomcat 미터는 설정 스위치에 걸려 있어 두 미터가 함께 와야 한 행이 값을 냅니다. */
    @Test
    @DisplayName("tomcat busy 와 정원이 함께 오면 한 행의 사용률이 된다")
    void tomcatRowsCollapseIntoOneUtilization() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(
                        resource(MetricAggregation.TOMCAT_BUSY, 40d, "api-1"),
                        resource(MetricAggregation.TOMCAT_MAX, 200d, "api-1"))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.saturation().resources().get(1).utilization().value()).isEqualTo(20.0);
    }

    /** CPU 는 이미 비율이라 인스턴스 안에서 나눌 것이 없고, 가장 위험한 인스턴스를 씁니다. */
    @Test
    @DisplayName("CPU 는 비율을 퍼센트로 바꿔 인스턴스 최댓값을 쓴다")
    void cpuUsesInstanceMaxAsPercent() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(
                        resource(MetricAggregation.CPU_USAGE, 0.12d, "api-1"),
                        resource(MetricAggregation.CPU_USAGE, 0.64d, "api-2"))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.saturation().resources().get(2).utilization().value()).isEqualTo(64.0);
    }

    /**
     * 합만 보면 한 대에 쏠린 것을 못 봅니다. 그리고 죽은 인스턴스의 마지막 값이 합에 남아 있는
     * 동안 활성 개수가 그 사실을 드러냅니다.
     */
    @Test
    @DisplayName("in-flight 는 합과 인스턴스 최댓값을 함께 내고 활성 개수를 센다")
    void inFlightSplitsSumAndInstanceMax() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(
                        resource(MetricAggregation.HTTP_IN_FLIGHT, 30d, "api-1"),
                        resource(MetricAggregation.HTTP_IN_FLIGHT, 90d, "api-2"),
                        resource(MetricAggregation.UP, 1d, "api-1"),
                        resource(MetricAggregation.UP, 1d, "api-2"),
                        resource(MetricAggregation.UP, 0d, "api-3"))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.saturation().inFlight().globalSum().value()).isEqualTo(120.0);
        assertThat(response.saturation().inFlight().instanceMax().value()).isEqualTo(90.0);
        assertThat(response.saturation().inFlight().instanceId()).isEqualTo("api-2");
        // up==0 인 인스턴스는 세지 않는다.
        assertThat(response.saturation().inFlight().activeInstances()).isEqualTo(2);
    }

    /** 어느 대가 최대인지 모르면 아무 대나 지목하지 않습니다. */
    @Test
    @DisplayName("in-flight 표본이 없으면 인스턴스를 지목하지 않는다")
    void inFlightWithoutSamplesNamesNoInstance() {
        AdminMetricsResponse response = assemble(FakePromQuery.empty(), globalQuery());

        assertThat(response.saturation().inFlight().instanceId()).isEmpty();
        assertThat(response.saturation().inFlight().globalSum().state())
                .isEqualTo(SourceStatus.PENDING);
    }

    // ── 큐 3영역 ───────────────────────────────────────────────────────────────

    /** zone 이름이 계약입니다 — 프론트가 이 값으로 분기합니다. */
    @Test
    @DisplayName("큐는 세 영역으로 나뉘고 영역 이름이 계약대로 나간다")
    void queuesCarryThreeContractZones() {
        AdminMetricsResponse response = assemble(FakePromQuery.empty(), globalQuery());

        // 상수 이름이 아니라 <b>응답에 실리는 값</b>을 본다. 상수는 리네임할 수 있지만
        // 이 문자열은 프론트 분기가 물고 있는 계약이다.
        assertThat(response.saturation().queues())
                .extracting(zone -> zone.zone().jsonValue())
                .containsExactly("Admission", "Persistence", "Telemetry");
        assertThat(response.saturation().queues().get(0).metrics())
                .extracting(AdminMetricsResponse.QueueMetric::label)
                .containsExactly("대기 인원", "입장 처리", "증가율", "해소 예상");
    }

    /**
     * 원천이 없는 것과 큐가 빈 것은 다른 사건입니다. 0 으로 내리면 화면이 "저장 대기 없음" 을
     * 그리는데, 실제로는 Kafka lag 을 아직 읽지도 못하는 상태입니다.
     */
    @Test
    @DisplayName("원천이 없는 영역은 0 이 아니라 PENDING 이다")
    void zonesWithoutSourceStayPending() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(resource(MetricAggregation.HTTP_IN_FLIGHT, 3d, "api-1"))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.saturation().queues().get(1).metrics()).allSatisfy(metric -> {
            assertThat(metric.value().state()).isEqualTo(SourceStatus.PENDING);
            assertThat(metric.value().value()).isNull();
        });
        assertThat(response.saturation().queues().get(2).metrics().get(0).value().state())
                .isEqualTo(SourceStatus.PENDING);
    }

    /** 대기 인원은 값 미터와 상태 미터를 짝으로 읽습니다 — batch 는 값이 없을 때 NaN 을 싣습니다. */
    @Test
    @DisplayName("대기 인원은 값·상태 짝으로 읽고 재고 수집 경로 시각을 쓴다")
    void admissionWaitingReadsPairedQueueMeter() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "app_observation_collect_last_success_epoch",
                List.of(stockCollectSuccess(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(
                        domain(MetricAggregation.QUEUE_LENGTH, Map.of(), 1200d),
                        domain(MetricAggregation.QUEUE_LENGTH_STATE, Map.of(),
                                SourceStatusCode.of(SourceStatus.VALID)))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        AdminMetricsResponse.QueueMetric waiting =
                response.saturation().queues().get(0).metrics().get(0);
        assertThat(waiting.value().state()).isEqualTo(SourceStatus.VALID);
        assertThat(waiting.value().value()).isEqualTo(1200.0);
    }

    /** 대기열을 쓰지 않는 회차는 장애가 아닙니다. batch 가 실어 보낸 이유가 그대로 나갑니다. */
    @Test
    @DisplayName("대기열이 없는 회차의 대기 인원은 batch 가 보낸 상태 그대로 나간다")
    void admissionWaitingKeepsReportedState() {
        FakePromQuery client = respond(Map.of(
                "timestamp(", List.of(age(FRESH_AGE_SECONDS)),
                "app_observation_collect_last_success_epoch",
                List.of(stockCollectSuccess(FRESH_AGE_SECONDS)),
                "process_cpu_usage", List.of(
                        domain(MetricAggregation.QUEUE_LENGTH, Map.of(), Double.NaN),
                        domain(MetricAggregation.QUEUE_LENGTH_STATE, Map.of(),
                                SourceStatusCode.of(SourceStatus.N_A)))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.saturation().queues().get(0).metrics().get(0).value().state())
                .isEqualTo(SourceStatus.N_A);
    }

    /** 질의 하나가 죽어도 행 자체는 남아야 화면이 자리를 잃지 않습니다. */
    @Test
    @DisplayName("질의가 실패해도 자원 행과 큐 영역은 그대로 있고 값만 UNAVAILABLE 이다")
    void saturationSurvivesQueryFailure() {
        AdminMetricsResponse response = assemble(FakePromQuery.down(), globalQuery());

        assertThat(response.saturation().resources()).hasSize(6);
        assertThat(response.saturation().resources().get(0).utilization().state())
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(response.saturation().inFlight().globalSum().state())
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(response.saturation().inFlight().activeInstances()).isZero();
        assertThat(response.saturation().queues()).hasSize(3);
    }

    /**
     * batch 도 CPU·heap·HikariCP 를 냅니다(실측). 라벨로 자르지 않으면 관측기 자신의 수치가
     * 발급 경로 자원 행에 섞이는데, 값이 정상 범위라 아무도 못 알아챕니다.
     */
    @Test
    @DisplayName("자원 셀렉터는 api 인스턴스로 좁히고 큐 셀렉터는 좁히지 않는다")
    void resourceSelectorIsScopedToApiJob() {
        FakePromQuery client = FakePromQuery.empty();
        assemble(client, globalQuery());

        String saturation = client.queries().stream()
                .filter(q -> q.contains(MetricAggregation.CPU_USAGE))
                .findFirst().orElseThrow();
        assertThat(saturation).contains("job=\"api\"");
        assertThat(saturation).contains(MetricAggregation.TOMCAT_BUSY);
        // 큐 길이는 batch 가 유일한 원천이다. api 로 좁히면 표본이 통째로 사라진다.
        assertThat(saturation).contains(" or {__name__=~\"" + MetricAggregation.QUEUE_LENGTH);
    }

    // ── 도우미 ─────────────────────────────────────────────────────────────────

    private static AdminMetricsResponse assemble(PromQuery client, MetricsQuery query) {
        return new PromMetricsAssembler(client, FIXED_TIME, STALE_AFTER, BUDGET).assemble(query);
    }

    private static MetricsQuery globalQuery() {
        return new MetricsQuery(MetricsWindow.ONE_MINUTE, null, null);
    }

    private static String percentileQuery(FakePromQuery client) {
        return client.queries().stream().filter(q -> q.contains("quantile!=")).findFirst().orElseThrow();
    }

    private static String rateQuery(FakePromQuery client) {
        return client.queries().stream().filter(q -> q.startsWith("rate(")).findFirst().orElseThrow();
    }

    /** 질의 문자열에 포함된 조각으로 응답을 고르는 fake 를 만듭니다. */
    private static FakePromQuery respond(Map<String, List<PromSample>> byQueryFragment) {
        return new FakePromQuery(promQl -> {
            List<PromSample> matched = new ArrayList<>();
            byQueryFragment.forEach((fragment, samples) -> {
                if (promQl.contains(fragment)) {
                    matched.addAll(samples);
                }
            });
            return List.copyOf(matched);
        });
    }

    private static ObservedValue<Double> rateOf(ErrorMetrics errors, ErrorClassKey key) {
        return errorClass(errors, key).rate();
    }

    private static boolean excluded(ErrorMetrics errors, ErrorClassKey key) {
        return errorClass(errors, key).excludedFromNumerator();
    }

    private static ErrorClass errorClass(ErrorMetrics errors, ErrorClassKey key) {
        return errors.classes().stream()
                .filter(candidate -> candidate.key() == key)
                .findFirst()
                .orElseThrow(() -> new AssertionError("분류가 응답에 없습니다: " + key));
    }

    /** {@code sum by (outcome) (rate(...))} 결과. 집계가 __name__ 을 지운다. */
    private static PromSample outcome(ReasonCode reasonCode, double rps) {
        return new PromSample("", Map.of("outcome", reasonCode.name()), rps, EVALUATED);
    }

    private static PromSample rate(String uriGroup, String result, double value, String instance) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("uri_group", uriGroup);
        labels.put("result", result);
        labels.put("instance", instance);
        // rate() 결과에는 __name__ 이 없다. 실제 Prometheus 응답과 같게 둔다.
        return new PromSample("", labels, value, EVALUATED);
    }

    /**
     * 모든 URI 그룹 × 모든 결과 분류가 0 인 결과 rate 입니다 — <b>요청이 한 건도 없는 상태</b>.
     *
     * <p>분류를 손으로 적지 않고 enum 전종을 돕니다. 하나라도 빠지면 그 축의 분모가 "0" 이
     * 아니라 "모름" 이 되어 판정이 조용히 PENDING 으로 새고, 그러면 이 테스트가 지키려는 것이
     * 사라집니다.</p>
     */
    private static List<PromSample> zeroRates() {
        List<PromSample> samples = new ArrayList<>();
        for (UriGroup uriGroup : UriGroup.values()) {
            for (ResultClass resultClass : ResultClass.values()) {
                samples.add(rate(uriGroup.tagValue(),
                        resultClass.name().toLowerCase(Locale.ROOT), 0d, "api-1"));
            }
        }
        return List.copyOf(samples);
    }

    private static PromSample quantile(String quantile, double seconds, String instance) {
        return quantile(UriGroup.ISSUE, quantile, seconds, instance);
    }

    private static PromSample quantile(
            UriGroup uriGroup, String quantile, double seconds, String instance) {
        return quantile(uriGroup, "success", quantile, seconds, instance);
    }

    /** {@code instance} 가 빈 문자열이면 원천이 라벨을 떨군 표본이다. */
    private static PromSample quantile(
            UriGroup uriGroup, String outcome, String quantile, double seconds, String instance) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("uri_group", uriGroup.tagValue());
        labels.put("outcome", outcome);
        labels.put("quantile", quantile);
        if (!instance.isEmpty()) {
            labels.put("instance", instance);
        }
        return new PromSample(MetricAggregation.HTTP_LATENCY_SECONDS, labels, seconds, EVALUATED);
    }

    private static ObservedValue<LatencyPercentiles> group(LatencyMetrics latency, UriGroup uriGroup) {
        return latency.groups().stream()
                .filter(candidate -> candidate.group().equals(uriGroup.tagValue()))
                .findFirst()
                .map(LatencyGroupStat::percentiles)
                .orElseThrow(() -> new AssertionError("그룹이 응답에 없습니다: " + uriGroup));
    }

    /** {@code max(time() - timestamp(...))} 결과. 라벨도 __name__ 도 없다. */
    private static PromSample age(long ageSeconds) {
        return new PromSample("", Map.of(), ageSeconds, EVALUATED);
    }

    private static PromSample collectSuccess(long ageSeconds) {
        return domain(MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH,
                Map.of(DomainMeterNames.TAG_COLLECT_PATH, DomainMeterNames.PATH_CONSISTENCY),
                EVALUATED.minusSeconds(ageSeconds).getEpochSecond());
    }

    /** batch 의 evaluationGauge 는 평가 미터에 phase=live 를 붙인다. */
    private static Map<String, String> liveGap(String type) {
        return Map.of(DomainMeterNames.TAG_GAP_TYPE, type,
                DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE);
    }

    private static Map<String, String> live() {
        return Map.of(DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE);
    }

    private static PromSample gap(String type, double value) {
        return domain(MetricAggregation.CONSISTENCY_GAP, liveGap(type), value);
    }

    private static PromSample gapState(String type, SourceStatus status) {
        return domain(MetricAggregation.CONSISTENCY_GAP_STATE, liveGap(type),
                SourceStatusCode.of(status));
    }

    private static PromSample resource(String metricName, double value, String instance) {
        return new PromSample(metricName, Map.of("job", "api", "instance", instance), value, EVALUATED);
    }

    private static PromSample pool(String metricName, double value, String instance, String pool) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("job", "api");
        labels.put("instance", instance);
        labels.put("pool", pool);
        return new PromSample(metricName, labels, value, EVALUATED);
    }

    private static PromSample memory(String metricName, String area, String id, double value) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("job", "api");
        labels.put("instance", "api-1");
        labels.put("area", area);
        labels.put("id", id);
        return new PromSample(metricName, labels, value, EVALUATED);
    }

    /** 재고 수집 경로의 마지막 성공 시각. 대기열 길이의 관측 시각은 이쪽이다. */
    private static PromSample stockCollectSuccess(long ageSeconds) {
        return domain(MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH,
                Map.of(DomainMeterNames.TAG_COLLECT_PATH, DomainMeterNames.PATH_STOCK),
                EVALUATED.minusSeconds(ageSeconds).getEpochSecond());
    }

    private static PromSample domain(String metricName, Map<String, String> labels, double value) {
        return new PromSample(metricName, labels, value, EVALUATED);
    }
}
