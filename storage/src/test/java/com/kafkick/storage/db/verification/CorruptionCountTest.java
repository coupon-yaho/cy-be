// 심은 오염 수를 정답 표에서 되짚어 세는 계산이 계약대로인지 확인합니다.
package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.FindingType;
import com.kafkick.storage.db.RepositoryTest;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * <b>심은 오염 수는 정답 행수와 다르다.</b> 오염 하나가 규칙 여럿을 어길 수 있어서,
 * 지금 시드에서는 오염 700 이 위반 800 을 낳는다. 화면이
 * <i>"오염 700건이 낳는 위반 800건을 전부 잡음"</i> 으로 그리려면 그 700 이 필요하다.
 *
 * <p><b>그 값을 어디서 얻느냐가 이 클래스의 전부다.</b> 세 후보가 있었다.
 *
 * <ol>
 *   <li>프론트가 {@code 종류 수 × 100} 으로 <b>추정</b> — 종류당 건수가 같다는 가정이라,
 *       시드가 한 종류만 200건 심는 날 화면만 조용히 틀린다.</li>
 *   <li>{@code docs/contract.json} 의 {@code corruption.injections} 를 <b>그대로 싣기</b> —
 *       그것은 <b>선언</b>이지 측정이 아니다. 시드가 바뀌고 계약을 안 고치면 하드코딩과 똑같다.</li>
 *   <li><b>DB 에서 센다</b> — 이 구현이 고른 것.</li>
 * </ol>
 *
 * <p>세는 방식은 가정 하나만 쓴다: <b>오염 하나가 자기 규칙 목록마다 정확히 한 행씩 낳는다.</b>
 * 그래서 종류별로 {@code 행수 ÷ 그 종류가 쓴 규칙 수} 를 더하면 오염 수가 된다.
 *
 * <p><b>그 가정은 계약이 적어 둔 것</b>({@code corruption.matrix})이라, 아래 마지막 검사가
 * 두 값을 맞대 본다 — 구현이 세는 값과 계약이 선언한 값이 갈리면 빨개진다. 둘 다 틀리는
 * 경우는 못 잡지만, <b>한쪽만 바뀌는 경우</b>가 실제로 일어나는 사고다.
 */
@RepositoryTest
@Import(ExpectedFindingJdbcAdapter.class)
class CorruptionCountTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);
    private static final long SEED_RUN = 1L;

    /** {@code batch} 모듈의 대조 테스트들이 쓰는 경로 규약과 같다 — 작업 디렉터리는 모듈 루트다. */
    private static final Path CONTRACT = Path.of("..", "docs", "contract.json");

    @Autowired
    private ExpectedFindingJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    @DisplayName("정답 묶음이 없으면 0이다 — exists 로 먼저 거르는 것이 전제다")
    void countsZeroWhenManifestMissing() {
        assertThat(adapter.corruptionCountOf(SEED_RUN)).isZero();
    }

    @Test
    @DisplayName("규칙 하나만 어기는 오염은 행수가 곧 오염 수다")
    void countsOneRowPerCorruptionWhenSingleRule() {
        rows(1, FindingType.STOCK_MISMATCH, 5);

        assertThat(adapter.corruptionCountOf(SEED_RUN)).isEqualTo(5);
    }

    /**
     * <b>이 검사가 이 클래스의 이유다.</b> 행을 그냥 세면 10 이 나오는데 심은 오염은 5 다.
     * {@code corrupt_type 3}(CANCEL_USE 이중 기록)이 실제로 이 모양이다 —
     * {@code ILLEGAL_TRANSITION} 과 {@code STOCK_MISMATCH} 를 한 번에 낳는다.
     */
    @Test
    @DisplayName("규칙 둘을 어기는 오염은 행수의 절반이다 — 행을 세면 두 배가 된다")
    void halvesRowsWhenOneCorruptionBreaksTwoRules() {
        rows(3, FindingType.ILLEGAL_TRANSITION, 5);
        rows(3, FindingType.STOCK_MISMATCH, 5);

        assertThat(rowCount())
                .as("전제가 깨졌다 — 행이 10이 아니면 이 검사가 뜻이 없다")
                .isEqualTo(10);
        assertThat(adapter.corruptionCountOf(SEED_RUN)).isEqualTo(5);
    }

    /**
     * <b>종류당 건수가 같다는 가정을 안 쓴다는 것을 여기서 잰다.</b> 종류마다 100건씩이라는
     * 전제로 세면({@code 종류 수 × 100}) 이 상황에서 틀린다.
     */
    @Test
    @DisplayName("종류마다 건수가 달라도 따라온다 — 종류 수 × 상수로 세지 않는다")
    void followsUnevenCountsPerType() {
        rows(1, FindingType.STOCK_MISMATCH, 7);
        rows(2, FindingType.REPLAY_MISMATCH, 3);
        rows(4, FindingType.ILLEGAL_TRANSITION, 11);

        assertThat(adapter.corruptionCountOf(SEED_RUN)).isEqualTo(21);
    }

    /**
     * <b>계약이 깨지면 그럴듯한 숫자를 안 내보낸다.</b> 규칙별 행수가 갈린 순간
     * "심은 오염 수" 라는 값이 무엇의 개수도 아니게 되는데, 그것을 화면과 매일 커밋되는
     * 리포트에 <b>틀렸다는 표시 없이</b> 실으면 아무도 못 본다.
     *
     * <p>이 필드를 만든 이유가 <i>"틀린 숫자보다 없는 숫자가 낫다"</i> 였으므로 같은 기준을
     * 지킨다.
     */
    @Test
    @DisplayName("규칙별 행수가 갈리면 죽는다 — 그럴듯한 거짓말을 안 싣는다")
    void refusesWhenRulesAreUneven() {
        rows(3, FindingType.ILLEGAL_TRANSITION, 3);
        rows(3, FindingType.STOCK_MISMATCH, 2);

        assertThatThrownBy(() -> adapter.corruptionCountOf(SEED_RUN))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("편차=1");
    }

    /**
     * <b>이 검사가 앞선 구현을 잡았다.</b> 한때 {@code SUM(행수 / 규칙 수)} 로 두고
     * <i>"소수부가 남으면 계약이 깨진 것"</i> 이라 적었는데, <b>그 검사가 계약보다 약했다</b> —
     * 4행·2행은 합 6 ÷ 규칙 2 = 3 으로 <b>딱 떨어져서 통과한다.</b> 깨진 계약이 정상
     * 오염 수로 나가는 자리였다.
     *
     * <p>계약이 요구하는 것은 나눗셈이 떨어지는 것이 아니라 <b>규칙별 행수가 같은 것</b>이다.
     */
    @Test
    @DisplayName("나눗셈이 떨어져도 규칙별로 갈렸으면 죽는다 — 소수부 검사로는 못 잡는다")
    void refusesEvenlyDivisibleButUnevenRules() {
        rows(3, FindingType.ILLEGAL_TRANSITION, 4);
        rows(3, FindingType.STOCK_MISMATCH, 2);

        assertThat(rowCount() % 2)
                .as("전제가 깨졌다 — 총 행수가 규칙 수로 나눠떨어져야 이 검사가 뜻이 있다")
                .isZero();
        assertThatThrownBy(() -> adapter.corruptionCountOf(SEED_RUN))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("편차=2");
    }

    @Test
    @DisplayName("다른 시드 실행의 정답은 안 센다")
    void ignoresOtherSeedRuns() {
        rows(1, FindingType.STOCK_MISMATCH, 4);
        rowsFor(SEED_RUN + 1, 1, FindingType.STOCK_MISMATCH, 99);

        assertThat(adapter.corruptionCountOf(SEED_RUN)).isEqualTo(4);
    }

    /**
     * <b>이 검사가 재는 것을 정확히 적는다.</b> {@code corruption.matrix} 를 그대로 심고
     * 세어 나온 값이 같은 파일의 {@code corruption.injections} 와 맞는지 본다.
     *
     * <p>그러므로 이것이 잡는 것은 둘이다 — <b>계약 파일 안에서 matrix 와 injections 가
     * 어긋나는 것</b>, 그리고 <b>구현이 matrix 모양에서 injections 를 못 재현하는 것</b>.
     *
     * <p><b>잡지 못하는 것도 적는다.</b> 실제 시드({@code cy-seed})의 출력을 안 쓰므로
     * <b>시드만 바뀌고 계약이 그대로인 드리프트는 여기서 안 잡힌다.</b> 한때 이 자리에
     * <i>"시드가 오염 구성을 바꾸면서 계약을 안 고치면 갈린다"</i> 고 적었는데 <b>거짓이었다</b> —
     * 리뷰가 잡았다. 그 축은 오염셋에 실제로 검증을 돌릴 때 {@code missing}·{@code unexpected}
     * 가 드러내고, cy-seed 쪽 {@code verify.py} 도 같은 대조를 한다.
     *
     * <p>계약 값을 응답에 <b>싣지는 않는다</b> — 그러면 선언을 측정인 척 내보내는 것이다.
     * 여기서는 검사에만 쓴다.
     */
    @Test
    @DisplayName("계약의 matrix 로 심으면 계약의 injections 가 나온다")
    void reproducesContractInjectionsFromItsMatrix() throws IOException {
        JsonNode corruption = new ObjectMapper()
                .readTree(Files.readString(CONTRACT, StandardCharsets.UTF_8))
                .path("corruption");

        Map<Integer, Integer> plantedPerType = new LinkedHashMap<>();
        for (JsonNode entry : corruption.path("matrix")) {
            int corruptType = entry.path("corrupt_type").asInt();
            // 계약의 rows 를 그대로 심을 수는 없다(100행 × 8 = 800행). 비율만 지키면
            // 나눗셈이 같은 답을 내므로 10분의 1로 줄여 심는다.
            rows(corruptType, FindingType.valueOf(entry.path("finding_type").asString()),
                    entry.path("rows").asInt() / 10);
            plantedPerType.merge(corruptType, entry.path("rows").asInt() / 10, Integer::sum);
        }

        assertThat(plantedPerType)
                .as("계약의 matrix 가 비어 있으면 이 검사가 통과만 하고 아무것도 안 잰다")
                .isNotEmpty();
        assertThat(adapter.corruptionCountOf(SEED_RUN) * 10)
                .as("구현이 센 오염 수와 계약의 corruption.injections 가 갈렸다. "
                        + "시드 구성이 바뀌었으면 docs/contract.json 도 함께 고쳐야 한다")
                .isEqualTo(corruption.path("injections").asInt());
    }

    private int rowCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM expected_findings WHERE seed_run_id = :s")
                .param("s", SEED_RUN).query(Integer.class).single();
    }

    private void rows(int corruptType, FindingType type, int count) {
        rowsFor(SEED_RUN, corruptType, type, count);
    }

    /** {@code uk_expected(seed_run_id, finding_type, target_key)} 때문에 키가 겹치면 안 된다. */
    private void rowsFor(long seedRunId, int corruptType, FindingType type, int count) {
        for (int i = 0; i < count; i++) {
            jdbcClient.sql("""
                            INSERT INTO expected_findings
                                (seed_run_id, corrupt_type, finding_type, target_key, created_at)
                            VALUES (:seedRunId, :corruptType, :findingType, :targetKey, :createdAt)
                            """)
                    .param("seedRunId", seedRunId)
                    .param("corruptType", corruptType)
                    .param("findingType", type.name())
                    .param("targetKey", "ISSUANCE:" + seedRunId + ":" + corruptType + ":" + i)
                    .param("createdAt", AS_OF)
                    .update();
        }
    }
}
