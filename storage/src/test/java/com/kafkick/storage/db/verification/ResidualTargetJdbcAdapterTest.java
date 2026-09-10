// 전후 비교의 대상 목록이 집계와 같은 말을 하는지, 페이지가 겹치거나 새지 않는지입니다.
package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ResidualCount;
import com.kafkick.core.verification.ResidualCursor;
import com.kafkick.core.verification.ResidualKind;
import com.kafkick.core.verification.ResidualTarget;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationFindingRepository;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>목록이 집계와 같은 말을 해야 한다.</b>
 *
 * <p>둘은 같은 사실을 다른 그레인으로 본다. 갈리면 화면이 <i>"3건 남았다"</i> 라고
 * 적어 놓고 목록에는 두 줄만 보여 준다 — <b>어느 쪽을 믿을지 아무도 모른다.</b>
 * 그래서 여기서 두 API 를 맞대는 것이 이 파일의 중심이다.
 */
@RepositoryTest
@Import({VerificationFindingJdbcAdapter.class, VerificationRunJdbcAdapter.class})
class ResidualTargetJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);

    @Autowired VerificationFindingJdbcAdapter findings;
    @Autowired VerificationRunJdbcAdapter runs;
    @Autowired JdbcClient jdbcClient;

    private long before;
    private long after;

    @BeforeEach
    void seed() {
        before = newRun(1);
        after = newRun(2);
        // 잔여 2 · 해소 1 · 신규 2 — **세 수가 서로 달라야 한다.** 신규와 해소가
        // 같으면 그 둘을 뒤바꾼 구현이 모든 단언을 그대로 통과한다(실제로 그랬다).
        insert(before, FindingType.STOCK_MISMATCH, "COUPON:1", "COUPON:2", "COUPON:3");
        insert(after, FindingType.STOCK_MISMATCH,
                "COUPON:1", "COUPON:2", "COUPON:4", "COUPON:5");
        // 다른 유형도 하나 — 유형 경계를 넘는 정렬을 태우려면 둘 이상이어야 한다.
        insert(before, FindingType.DUP_PER_MEMBER, "COUPON:9|MEMBER:1");
        insert(after, FindingType.DUP_PER_MEMBER, "COUPON:9|MEMBER:1");
    }

    /**
     * <b>이 파일의 중심.</b> 목록을 종류별로 세면 집계와 한 자리도 안 틀려야 한다.
     *
     * <p>어느 한쪽만 고치면 여기서 걸린다 — 예컨대 목록의 {@code HAVING} 을 잘못 짜
     * {@code INTRODUCED} 를 {@code RESOLVED} 로 세면 집계와 갈린다.
     */
    @Test
    @DisplayName("목록을 종류별로 센 값이 집계와 정확히 같다")
    void theTargetListAgreesWithTheAggregate() {
        Map<FindingType, ResidualCount> aggregate = findings.residualByType(before, after);

        for (FindingType type : List.of(FindingType.STOCK_MISMATCH, FindingType.DUP_PER_MEMBER)) {
            ResidualCount counted = aggregate.get(type);
            assertThat(countOf(type, ResidualKind.PERSISTED))
                    .as("%s 잔여", type).isEqualTo(counted.persisted());
            assertThat(countOf(type, ResidualKind.INTRODUCED))
                    .as("%s 신규", type).isEqualTo(counted.introduced());
            assertThat(countOf(type, ResidualKind.RESOLVED))
                    .as("%s 해소", type).isEqualTo(counted.resolved());
        }
    }

    /**
     * <b>한 줄씩 넘겨도 전부를 정확히 한 번씩 본다.</b>
     *
     * <p>페이지 크기 1 이 가장 가혹하다 — 경계가 매 줄마다 생기므로 커서가 한 칸이라도
     * 어긋나면 <b>겹치거나 샌다.</b>
     */
    @Test
    @DisplayName("한 줄씩 넘겨 모은 것이 한 번에 읽은 것과 같다")
    void pagingOneRowAtATimeYieldsExactlyTheSameSet() {
        List<ResidualTarget> whole = findings.residualTargets(before, after, null, null, 1_000);

        List<ResidualTarget> paged = new ArrayList<>();
        ResidualCursor cursor = null;
        for (int guard = 0; guard < 100; guard++) {
            List<ResidualTarget> page = findings.residualTargets(before, after, null, cursor, 1);
            if (page.isEmpty()) {
                break;
            }
            paged.addAll(page);
            cursor = ResidualCursor.after(page.get(page.size() - 1));
        }

        assertThat(paged).containsExactlyElementsOf(whole);
    }

    /**
     * <b>한 대상이 페이지 경계에서 쪼개지면 안 된다.</b>
     *
     * <p>커서를 {@code (유형, 대상키)} 가 아닌 다른 축으로 자르면 같은 대상의 앞뒤
     * 실행이 서로 다른 페이지로 갈려, <b>{@code PERSISTED} 하나가
     * {@code INTRODUCED} 와 {@code RESOLVED} 둘로 보인다.</b>
     */
    @Test
    @DisplayName("같은 대상이 두 종류로 나뉘어 나오지 않는다")
    void aTargetNeverAppearsUnderTwoKinds() {
        List<ResidualTarget> all = findings.residualTargets(before, after, null, null, 1_000);

        assertThat(all).extracting(t -> t.type() + "|" + t.targetKey()).doesNotHaveDuplicates();
    }

    /**
     * <b>대소문자가 다르면 다른 대상이다.</b>
     *
     * <p>{@code target_key} 의 콜레이션이 {@code utf8mb4_0900_ai_ci} 라 비교가 기본으로
     * 대소문자를 <b>안 가린다.</b> {@code GROUP BY}·{@code ORDER BY}·커서 셋 중 하나라도
     * {@code CAST(... AS BINARY)} 를 빠뜨리면 두 대상이 한 줄로 접힌다.
     */
    @Test
    @DisplayName("대소문자만 다른 대상 키를 같은 것으로 접지 않는다")
    void keysDifferingOnlyByCaseAreDistinctTargets() {
        long a = newRun(3);
        long b = newRun(4);
        insert(a, FindingType.REPLAY_MISMATCH, "ISSUANCE:abc");
        insert(b, FindingType.REPLAY_MISMATCH, "ISSUANCE:ABC");

        List<ResidualTarget> all = findings.residualTargets(a, b, null, null, 1_000);

        assertThat(all)
                .as("접히면 잔여 1건으로 보이는데, 실제로는 해소 1 + 신규 1 이다")
                .hasSize(2)
                .extracting(ResidualTarget::kind)
                .containsExactlyInAnyOrder(ResidualKind.RESOLVED, ResidualKind.INTRODUCED);
    }

    /**
     * <b>대소문자와 커서를 <i>겹쳐서</i> 태운다.</b>
     *
     * <p>둘을 따로 보면 커서 비교의 {@code CAST(... AS BINARY)} 를 빼도 아무 데서도
     * 안 걸린다(실제로 그랬다) — 대소문자 시험은 커서를 안 쓰고, 페이징 시험은
     * 대소문자가 다른 키를 안 쓴다.
     *
     * <p>빠지면 커서가 {@code utf8mb4_0900_ai_ci} 로 비교돼 {@code 'ISSUANCE:ABC'} 를
     * 지난 커서가 {@code 'ISSUANCE:abc'} 도 <b>같은 값으로 보고 건너뛴다.</b>
     * 한 줄이 조용히 사라지는데, 그 줄이 운영자가 조치해야 할 대상이다.
     *
     * <p>⚠️ <b>두 키를 같은 실행에 못 넣는다 — 재 보고 알았다.</b>
     * {@code uk_run_finding (run_id, finding_type, target_key)} 도 같은 콜레이션이라
     * <b>DB 가 둘을 같은 키로 보고 두 번째 삽입을 거부한다.</b> 그래서 이 형상은
     * 실행을 갈라야만 만들어진다 — 그리고 그것이 곧 커서가 유형 경계 안에서
     * 두 그룹을 넘어가는 경우다.
     */
    @Test
    @DisplayName("대소문자만 다른 키가 섞여 있어도 한 줄씩 넘겨 전부 본다")
    void pagingDoesNotSkipKeysThatDifferOnlyByCase() {
        long a = newRun(5);
        long b = newRun(6);
        insert(a, FindingType.REPLAY_MISMATCH, "ISSUANCE:ABC", "ISSUANCE:z");
        insert(b, FindingType.REPLAY_MISMATCH, "ISSUANCE:abc", "ISSUANCE:z");

        List<ResidualTarget> whole = findings.residualTargets(a, b, null, null, 1_000);

        List<ResidualTarget> paged = new ArrayList<>();
        ResidualCursor cursor = null;
        for (int guard = 0; guard < 20; guard++) {
            List<ResidualTarget> page = findings.residualTargets(a, b, null, cursor, 1);
            if (page.isEmpty()) {
                break;
            }
            paged.addAll(page);
            cursor = ResidualCursor.after(page.get(page.size() - 1));
        }

        assertThat(whole)
                .as("ABC(해소) · abc(신규) · z(잔여) 셋이 나와야 이 단언이 뜻이 있습니다")
                .hasSize(3);
        assertThat(paged)
                .as("커서가 대소문자를 안 가리면 여기서 한 줄이 사라집니다")
                .containsExactlyElementsOf(whole);
    }

    /**
     * <b>세 종류를 다 태운다.</b> 하나만 보면 나머지 둘을 뒤바꾼 구현이 통과한다 —
     * 좁히는 절({@code HAVING})과 줄의 종류를 정하는 계산({@code ResidualKind.of})이
     * <b>서로 다른 자리</b>라 갈릴 수 있다.
     */
    @Test
    @DisplayName("종류를 좁히면 그 종류만 나오고, 세 종류가 다 그렇다")
    void theKindFilterReturnsOnlyThatKind() {
        for (ResidualKind kind : ResidualKind.values()) {
            List<ResidualTarget> page =
                    findings.residualTargets(before, after, kind, null, 1_000);

            assertThat(page).as("%s 가 하나도 안 나오면 이 단언이 아무것도 안 봅니다", kind)
                    .isNotEmpty();
            assertThat(page).as("%s 로 좁혔는데 다른 종류가 섞였습니다", kind)
                    .allSatisfy(t -> assertThat(t.kind()).isEqualTo(kind));
        }
    }

    /**
     * 같은 실행끼리 맞대면 <b>모든 대상이 잔여</b>로 나온다 — 조용한 오답이라 끊는다.
     * 집계({@code residualByType})와 <b>같은 코드</b>여야 두 자리가 다른 답을 안 낸다.
     */
    @Test
    @DisplayName("같은 실행끼리는 맞댈 수 없다")
    void refusesToCompareARunWithItself() {
        assertThatThrownBy(() -> findings.residualTargets(before, before, null, null, 10))
                .isInstanceOf(BusinessException.class);
    }

    @Test
    @DisplayName("페이지 크기가 범위 밖이면 거부한다")
    void refusesAPageSizeOutsideTheCap() {
        assertThatThrownBy(() -> findings.residualTargets(before, after, null, null, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> findings.residualTargets(before, after, null, null,
                VerificationFindingRepository.MAX_TARGET_PAGE + 1))
                .as("상한 없이 부르면 12만 키 형상에서 2.7MB 를 한 응답에 싣는다")
                .isInstanceOf(IllegalArgumentException.class);
    }

    private long countOf(FindingType type, ResidualKind kind) {
        return findings.residualTargets(before, after, kind, null, 1_000).stream()
                .filter(t -> t.type() == type)
                .count();
    }

    private void insert(long runId, FindingType type, String... keys) {
        for (String key : keys) {
            jdbcClient.sql("INSERT INTO verification_findings"
                            + " (run_id, finding_type, target_key, expected, actual)"
                            + " VALUES (?, ?, ?, '기대', '실제')")
                    .params(runId, type.name(), key).update();
        }
    }

    private long newRun(int attempt) {
        return runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, attempt, AS_OF)).id();
    }
}
