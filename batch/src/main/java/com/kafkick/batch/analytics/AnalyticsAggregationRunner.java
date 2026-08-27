package com.kafkick.batch.analytics;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.core.support.TimeProvider;

/**
 * 브랜드 분석 집계 배치(OBS-51). 세 축을 <b>증분</b>으로 다시 세어 집계 표에 Upsert 한다.
 *
 * <h2>경계</h2>
 *
 * <p>B 는 집계 표와 이 배치까지다. 이 표들을 읽어 {@code AdminAnalyticsDataset} 을 조립하는
 * {@code AdminAnalyticsSource} 구현과 Service·API 연결은 A 가 한다.
 *
 * <h2>재실행 결정성</h2>
 *
 * <p>{@code as_of} 를 파라미터로 받아 박는다. 배치 시작 시각을 기준으로 삼으면 같은 기간을 다시
 * 돌릴 때 값이 달라진다 — 재계수에 걸리는 {@code issued_at <= 끝점} 이 그 결정성의 전부다.
 * <b>포함 경계여야 한다</b> — 발견 창도 {@code created_at <= 끝점} 이고 ISSUE 이력은
 * {@code created_at = issued_at} 이라, 여기만 배타적으로 두면 끝점에 걸친 발급이 발견되고도
 * 안 세어지고 다음 걸음의 발견 창에도 안 들어와 영영 한 건 모자란다.
 *
 * <p>⚠️ 상태 축은 예외다. {@code issuances.status} 가 계속 바뀌므로 같은 발급일이라도 집계 시점마다
 * 값이 다르다. 그쪽의 답은 결정성이 아니라 {@code observed_at} 이다.
 *
 * <h2>왜 "더하기" 가 아니라 다시 세는가</h2>
 *
 * <p>{@code issue_count + 델타} 는 스캔이 없어 훨씬 싸지만 <b>멱등하지 않다</b> — 실패한 회차를
 * 다시 돌리면 또 더해진다. 재실행 결정성이 이 티켓의 완료 조건이라 재계수를 택했다.
 *
 * <h2>재계수 비용 — 실측</h2>
 *
 * <p>{@code issuances} 에는 {@code (coupon_id, id)} 만 있고 {@code issued_at} 이 인덱스에 없다.
 * 그래서 버킷 하나를 다시 세는 일은 <b>그 회차 행 전량 스캔</b>이다. 바뀐 날짜 수(D)에 비례해
 * D × 회차 전량이 되고, 정상 운영에서 D 는 1~2 지만 <b>밀린 뒤 재개될 때 커진다.</b>
 *
 * <p>실측(MySQL 8.4 · 컨테이너 · 버퍼풀 1GB · 유휴 · 한 회차 300만 행):
 *
 * <pre>
 *   바뀐 버킷 찾기 (창 = 이력 300만 전체)  2.74s
 *   바뀐 버킷 찾기 (창 = 20만 행)          0.07s
 *   뜨거운 버킷 재계수                     1.64s
 *   뜨거운 버킷 상태 재계수                1.80s
 * </pre>
 *
 * <p>즉 <b>한 걸음</b>은 0.07 + 1.8 ≈ 1.9초로 4초 상한 안이지만, 창을 안 자르면 4.4초로 넘긴다.
 * 그것이 걸음 나누기가 있는 이유다. 다만 재계수 1.64초는 창과 무관하게 <b>매 회차</b> 든다 —
 * 인덱스가 없어서다.
 *
 * <p>TODO(후속 티켓): 부하 <b>중</b>(경합 포함) 이 문장의 p99 를 재고, {@code issuances} 에
 * {@code (coupon_id, issued_at)} 을 얹을지 판단한다. 이 티켓은 그 추가를 금지한다 —
 * 발급 경로의 쓰기 테이블이라 v1·v2·v3 처리량 측정을 바꾼다.
 *
 * <h2>이 배치가 <b>안</b> 막는 것</h2>
 *
 * <p>이 집계는 Spring Batch Job 이 아니라 {@code @Scheduled} 다. 그래서 {@code JOB_INST_UN} 의
 * 중복 실행 방지가 <b>안 걸린다</b> — 두 노드가 뜨면 같은 창을 둘 다 돈다. 값은 멱등이라
 * 틀리지 않고(두 축은 Upsert, 상태 축은 회차별 행) 낭비와 여분의 행만 생긴다. 노드를 늘릴
 * 계획이 생기면 그때 잠금을 다시 본다.
 */
public class AnalyticsAggregationRunner {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsAggregationRunner.class);

    private final AnalyticsAggregateReader reader;
    private final AnalyticsRunStore store;
    private final AnalyticsAggregationProperties properties;
    private final TimeProvider timeProvider;

    public AnalyticsAggregationRunner(
            AnalyticsAggregateReader reader,
            AnalyticsRunStore store,
            AnalyticsAggregationProperties properties,
            TimeProvider timeProvider
    ) {
        this.reader = reader;
        this.store = store;
        this.properties = properties;
        this.timeProvider = timeProvider;
    }

    /** 스케줄러 진입점. 기준 시각이 이 순간으로 박힌다. */
    public AnalyticsAggregationResult runOnce() {
        return runOnce(timeProvider.instant());
    }

    /**
     * 기준 시각을 받아 세 축을 집계한다.
     *
     * <p>축 하나가 실패해도 나머지 축은 계속 돈다. 실패한 축은 {@code UNAVAILABLE} 로 남고
     * <b>집계 수위선을 전진시키지 않는다</b> — A 의 {@code observedAt} 이 그 값이라, 전진시키면
     * 실패한 집계가 정상 관측으로 읽히고 stale-after 판정도 함께 어긋난다.
     */
    public AnalyticsAggregationResult runOnce(Instant asOf) {
        long runId = store.openRun(asOf, timeProvider.instant());
        Map<AnalyticsAxis, Integer> written = new EnumMap<>(AnalyticsAxis.class);
        Map<AnalyticsAxis, String> failures = new EnumMap<>(AnalyticsAxis.class);

        try {
            for (AnalyticsAxis axis : AnalyticsAxis.values()) {
                try {
                    written.put(axis, aggregate(axis, runId, asOf));
                } catch (RuntimeException exception) {
                    log.error("analytics axis failed: runId={}, axis={}", runId, axis, exception);
                    // 축 표시는 아래 closeRun 이 회차 마감과 같은 문장으로 적는다.
                    failures.put(axis, describe(exception));
                }
            }
        } finally {
            // ⚠️ finally 여야 한다. 밖에 두면 축 실패를 기록하다 운영 풀이 한 번 끊기는 것만으로
            //    회차가 IN_PROGRESS·failure_reason=NULL 로 영구히 남고, 남은 축은 시도조차 안 된다.
            store.closeRun(runId, failures);
        }

        log.info("analytics aggregation finished: runId={}, asOf={}, written={}, failed={}",
                runId, asOf, written, failures.keySet());
        return new AnalyticsAggregationResult(runId, asOf, written, failures);
    }

    /**
     * 한 축을 {@code asOf} 까지 <b>여러 걸음에 나눠</b> 따라잡는다.
     *
     * <p>걸음 하나는 이력 {@code max-window-rows} 행이다. 시간으로 자르지 않는 이유는
     * {@link AnalyticsAggregateReader#nextStepBoundary} 에 실측과 함께 적었다 — 부하 회차는 300만
     * 건이 30분에 몰려서, 시간 창은 아무리 좁혀도 그 안에 전부 들어온다.
     *
     * <p>걸음마다 커밋하고 집계 지점을 옮긴다. 그래서 회차가 중간에 죽어도 거기까지는 남고,
     * 다음 회차가 이어받는다. 한 회차의 걸음 수는 {@code max-steps-per-run} 으로 막는다 —
     * 안 막으면 한 번 크게 밀렸을 때 이 회차가 몇 시간을 붙잡고 관측 풀 커넥션을 물고 있는다.
     */
    private int aggregate(AnalyticsAxis axis, long runId, Instant asOf) {
        Instant through = store.watermark(axis);
        requireNotGoingBackwards(axis, through, asOf);

        int written = 0;
        int steps = 0;
        Instant cursor = through;
        // 늦은 커밋을 겹쳐 훑는 것은 **회차의 첫 걸음**에서 한 번이면 된다. 이력의 created_at 은
        // 이벤트 시각이라 커밋이 늦은 트랜잭션이 이미 지나간 시각으로 뒤늦게 보이는데, 뒤 걸음이
        // 보는 구간은 이 회차가 방금 읽은 곳이라 다시 볼 이유가 없다.
        //
        // ⚠️ 그래서 겹쳐 훑기가 실제로 덮는 것은 **직전 회차의 마지막 걸음 구간**이다. 여러 걸음으로
        //    따라잡은 회차의 중간 걸음 구간에 늦게 커밋된 이력이 있으면 그것은 안 잡힌다. 그 구간은
        //    이미 지난 과거라 늦은 커밋이 거기 떨어질 일이 사실상 없지만, A 와 합의한
        //    "lag 범위 안의 늦은 커밋 포함" 이 **따라잡기 구간에서는 마지막 걸음까지만** 참이라는
        //    뜻이다. 정상 운영(회차당 한 걸음)에서는 차이가 없다.
        Instant windowStart = through.minus(properties.watermarkLag());
        do {
            StepWindow window = nextWindow(windowStart, asOf, cursor);
            written += writeStep(axis, runId, window.since(), window.target());
            cursor = window.target();
            // 다음 걸음은 앞 걸음의 끝점에서 시작한다 — 겹치면 창이 max-window-rows 를 넘는다.
            windowStart = window.target();
            steps++;
        } while (cursor.isBefore(asOf) && steps < properties.maxStepsPerRun());

        if (cursor.isBefore(asOf)) {
            log.warn("analytics 축이 아직 따라잡는 중이다: axis={}, through={}, asOf={}, steps={}",
                    axis, cursor, asOf, steps);
        }
        return written;
    }

    /**
     * 걸음의 <b>양끝</b>을 함께 고른다. 끝점만 다시 잡고 하한을 그대로 두면 실제로 읽는 창이
     * 상한을 넘는다 — 예산을 행 수로 잡았으므로 하한과 끝점은 반드시 같이 움직여야 한다.
     *
     * <p>첫 걸음은 하한이 커서보다 lag 만큼 앞이라, lag 구간의 이력만으로 창이 다 차면 끝점이
     * 커서를 못 넘는다. 그때는 <b>겹쳐 훑기를 이번 걸음에서 포기하고</b>(하한도 커서로 당긴다)
     * 새 구간만 본다 — 늦은 커밋 재훑기는 다음 회차로 미뤄지고, 창은 상한 안에 남는다.
     *
     * <p>⚠️ 그래도 못 나가면 <b>무한 루프</b>다. 같은 마이크로초에 이력이 상한보다 많이 몰린
     * 경우에만 생기는데, 그때는 자르기를 포기하고 {@code asOf} 까지 간다 — 멈추는 것보다 낫다.
     */
    private StepWindow nextWindow(Instant windowStart, Instant asOf, Instant cursor) {
        if (!cursor.isBefore(asOf)) {
            // 새로 볼 구간이 없다 — 늦은 커밋 겹쳐 훑기만 남는다. 끝점을 고를 여지가 없으므로
            // 하한 쪽을 행 수로 묶는다. 안 묶으면 창이 lag 로만 정해져 예산 밖으로 나간다.
            return new StepWindow(
                    reader.boundedWindowStart(windowStart, asOf, properties.maxWindowRows()), asOf);
        }
        Instant target = reader.nextStepBoundary(windowStart, asOf, properties.maxWindowRows());
        if (!target.isAfter(cursor) && windowStart.isBefore(cursor)) {
            log.warn("analytics 겹쳐 훑기만으로 창이 찼다. 이번 걸음은 새 구간만 본다: cursor={}", cursor);
            windowStart = cursor;
            target = reader.nextStepBoundary(cursor, asOf, properties.maxWindowRows());
        }
        if (!target.isAfter(cursor)) {
            log.warn("analytics 걸음이 전진하지 못해 창 자르기를 건너뛴다: cursor={}", cursor);
            target = asOf;
        }
        return new StepWindow(windowStart, target);
    }

    /** 한 걸음이 읽는 구간. 하한과 끝점이 따로 놀면 창 크기 예산이 무너진다. */
    private record StepWindow(Instant since, Instant target) {}

    private int writeStep(AnalyticsAxis axis, long runId, Instant since, Instant target) {
        // 시각이 세 개라 헷갈리기 쉬워 여기 모아 둔다.
        //   observed_at(행)          — 이 값을 **읽은** 시점. 되짚을 때 쓴다
        //   completed_at(축)         — 이 축이 마지막으로 **커밋된** 시점. 운영 진단용
        //   aggregated_through(축)   — **어디까지 셌나**. A 의 observedAt 이고 STALE 판정의 기준
        // 커밋 시각을 행에 적으면 실제보다 신선해 보이므로 읽은 시점을 쓴다.
        Instant observedAt = timeProvider.instant();
        return switch (axis) {
            case MONTHLY_TREND -> store.writeDaily(
                    runId, reader.readDaily(since, target), timeProvider.instant(), target);
            case HOURLY_HEATMAP -> store.writeHourly(
                    runId, reader.readHourly(since, target), timeProvider.instant(), target);
            case ISSUANCE_STATUS -> store.writeStatuses(
                    runId, reader.readStatuses(since, target, observedAt),
                    timeProvider.instant(), target);
        };
    }

    /**
     * 기준 시각이 뒤로 가면 그 축을 <b>건드리지 않는다.</b>
     *
     * <p>재계수는 {@code issued_at <= as_of} 로 자르므로, 이미 센 지점보다 이른 기준으로 다시 세면
     * <b>더 작은 값</b>이 나온다. 그 값이 더 큰 run_id 를 달고 최신이 되어, 화면의 발급 수가
     * 조용히 줄고 되돌아오지 않는다. 시계가 뒤로 조정되면(NTP) 실제로 일어난다.
     *
     * <p>⚠️ 반대 방향 실패 — 시계가 뒤로 간 동안 이 축은 안 돈다. 대신 회차가 사유와 함께 FAILED 로
     * 남아 드러나고, 시계가 정상으로 돌아오면 저절로 재개된다. 조용히 줄어드는 쪽보다 낫다고 봤다.
     */
    private static void requireNotGoingBackwards(AnalyticsAxis axis, Instant through, Instant asOf) {
        if (asOf.isBefore(through)) {
            throw new AxisSkipped(
                    "as_of 가 축의 집계 지점보다 이르다: axis=" + axis
                            + ", aggregatedThrough=" + through + ", asOf=" + asOf);
        }
    }

    /**
     * 회차 이력에는 <b>예외 종류만</b> 남긴다.
     *
     * <p>원문 메시지에는 SQL 문장과 그 안의 데이터가 그대로 실린다(실측 — 이 배치를 만드는 중에
     * {@code Duplicate entry '2026-08-25-100-4'} 가 들어갔다). {@code failure_reason} 은 나중에
     * 관리자 화면으로 나갈 수 있는 값이라 원문을 영속화하지 않는다. 예외는 {@link AxisSkipped}
     * 뿐이다 — 그 메시지는 우리가 짓고 데이터를 안 담는다.
     *
     * <p>⚠️ 반대 방향 실패 — 이력만 보고는 원인을 못 짚는다. 전문은 같은 시점의 ERROR 로그가
     * 스택과 함께 갖고 있으므로, 되짚을 때는 runId 로 로그를 찾아야 한다.
     */
    private static String describe(RuntimeException exception) {
        return exception instanceof AxisSkipped
                ? exception.getClass().getSimpleName() + ": " + exception.getMessage()
                : exception.getClass().getSimpleName();
    }

    /**
     * 이 배치가 <b>스스로</b> 판단해 축을 건너뛸 때 던진다.
     *
     * <p>이 예외만 사유 원문을 회차 이력에 남긴다 — 메시지를 우리가 짓고, 축 이름과 시각 말고는
     * 아무 데이터도 담지 않기 때문이다. 밖에서 올라온 예외(특히 DB)는 종류만 남긴다.
     */
    static final class AxisSkipped extends IllegalStateException {
        AxisSkipped(String message) {
            super(message);
        }
    }
}
