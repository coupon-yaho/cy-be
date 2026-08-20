package com.kafkick;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.StreamUtils;

/**
 * 관측 대상 회차를 환경변수로 박는 것은 {@code benchmark_runs} 가 없기 때문에 쓰는 임시 방편이다.
 * 그 테이블이 생기는 순간이 곧 이 방편을 걷어낼 때다.
 *
 * <p>측정 시작(benchmark start)은 batch 가 이미 떠 있는 뒤에 일어난다. 그래서 환경변수로 박으려면
 * 회차마다 batch 를 재시작해야 한다. {@code benchmark_runs} 가 생기면 <b>진행 중인 행</b>
 * ({@code stopped_at IS NULL})에서 회차를 읽는 쪽이 맞다 — 재시작도, 사람이 값을 넣는 절차도 없어진다.
 *
 * <p>{@code benchmark_runs} 행에는 회차뿐 아니라 <b>엔진 버전</b>도 함께 들어간다
 * ({@code BenchmarkStartRequest} 가 engineVersion·queueMode 를 받는다). 지금은 batch 가 둘 다
 * 기동 시점 환경변수로 고정하고 있어, 측정 회차와 어긋나도 알 방법이 없다 — 그래서 지금 믿고 있는
 * 값을 {@code app.observation.engine.version} 으로 내보내 사람이 대조하게 해 뒀다.
 *
 * <p>이 테스트가 깨지면 OBS-14b 가 붙었다는 뜻이다. 그때 할 일 —
 * <ul>
 *   <li>{@code ConsistencyRawValueReader} 의 회차 선택을 진행 중인 run 기준으로 바꾼다.
 *   <li><b>엔진 버전도 같은 행에서 읽는다.</b> 회차 도중 런타임 토글이 바뀌어도 그 회차의 기준은
 *       시작할 때 박제된 값이라, 런타임 설정(OBS-19)보다 이쪽이 측정의 권위다.
 *   <li>{@code observation.domain-gauge.coupon-id} 는 그 조회를 무시하고 강제 지정하는
 *       수동 탈출구로만 남긴다(회차 없이 관측만 돌려 볼 때).
 *   <li>run 이 없는 동안의 동작을 정한다 — 지금처럼 "가장 최근 열린 회차" 인지, 값을 안 내는지.
 * </ul>
 */
class BatchBenchmarkRunsAssumptionTest {

    @Test
    @DisplayName("benchmark_runs 가 아직 없다 — 회차를 환경변수로 박는 임시 방편의 전제다")
    void benchmarkRunsTableDoesNotExistYet() throws Exception {
        Resource[] migrations = new PathMatchingResourcePatternResolver()
                .getResources("classpath*:db/migration/*.sql");

        assertThat(migrations).as("마이그레이션을 하나도 못 읽었다면 이 감시는 무효다").isNotEmpty();
        assertThat(Arrays.stream(migrations).filter(BatchBenchmarkRunsAssumptionTest::mentionsBenchmarkRuns))
                .as("benchmark_runs 가 생겼다. 회차 출처를 환경변수에서 진행 중인 run 으로 바꾼다"
                        + " — 클래스 주석의 세 항목을 보라")
                .isEmpty();
    }

    private static boolean mentionsBenchmarkRuns(Resource migration) {
        try {
            return StreamUtils.copyToString(migration.getInputStream(), StandardCharsets.UTF_8)
                    .toLowerCase()
                    .contains("benchmark_runs");
        } catch (Exception exception) {
            throw new IllegalStateException("마이그레이션을 읽지 못했다: " + migration, exception);
        }
    }
}
