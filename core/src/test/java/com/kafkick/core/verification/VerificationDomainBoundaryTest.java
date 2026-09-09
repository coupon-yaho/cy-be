// 사전예약이 그대로 가져갈 수 있는 절반이 쿠폰 어휘에 안 닿는지 확인합니다.
package com.kafkick.core.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import static java.util.Map.entry;

import java.util.Map;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>검증 틀의 절반은 쿠폰과 무관하고, 그 절반이 다른 도메인이 가져갈 수 있는 것이다.</b>
 *
 * <p>사전예약 PRD 는 <b>정합성 대사 배치</b>를 명시하고(§*"주기적으로 실행되는 배치가
 * 최종적으로 정정한다"*), KPI 여덟 중 <b>셋</b>이 그 대사의 산출물로 검증된다
 * (유실 0건 · 정합률 100% · 취소 후 외부 잔존 0건). 즉 그쪽에서도 대사는 지워지지 않는다.
 *
 * <p>그때 쿠폰 규칙(V1~V6)은 안 따라간다 — 예약은 자기 규칙 어휘를 갖는다. 따라가는 것은
 * <b>그 위</b>다: 실행 이력 · 주기/수동 트리거 · 중복 실행 방지 · 자동 회수 · 판정 어휘.
 * PRD 가 요구하는 것 중 <b>주기·수동 실행 / 중복 방지 / 실행 이력·보고서</b> 셋이 여기 있다.
 *
 * <h2>그 경계를 지금까지 아무것도 안 지켰다</h2>
 *
 * <p>측정은 손으로 했다 — {@code VerificationRun} 에 {@code FindingType} 하나를 import
 * 하면 경계가 죽는데 <b>아무것도 안 빨개진다.</b> 한 번 오염되면 되돌리기 어렵다:
 * 그 타입이 필드에 박히면 저장 스키마와 API 응답까지 따라 움직인다.
 *
 * <p>형제는 {@code CoreArchitectureTest} 다 — <i>"core 가 어댑터 타입을 알면 안 된다"</i> 를
 * 소스로 훑어 세고, 예외마다 <b>왜 예외인지</b>를 주석에 적는다. 같은 형태를 쓴다.
 *
 * <h2>왜 import 만 안 보고 낱말을 보나</h2>
 *
 * <p>{@code FindingType} 은 같은 패키지라 <b>import 가 안 붙는다.</b> import 만 세면
 * {@code core.verification} 안에서는 아무것도 못 잡는다 — 그래서 <b>낱말</b>을 센다.
 *
 * <p><b>주석은 걷어내고 센다.</b> {@code docs/19} 가 같은 축을 이미 세면서 그 규칙을
 * 정해 뒀다 — <i>"설명에 도메인 이름이 나오는 것과 타입·필드·SQL 이 도메인에 묶인 것은
 * 전혀 다른 문제다."</i> 재사용을 막는 것은 <b>타입과 필드</b>이지 문구가 아니다.
 * ⚠️ 처음에는 주석까지 셌는데, 그러면 이 가드가 <b>고칠 필요 없는 javadoc 을 고치게</b>
 * 만든다 — 같은 저장소의 문서와 가드가 경계의 뜻을 다르게 말하는 상태가 된다.
 */
class VerificationDomainBoundaryTest {

    private static final Path SOURCE_ROOT = Path.of("src", "main", "java",
            "com", "kafkick", "core", "verification");

    /**
     * <b>쿠폰 도메인의 낱말.</b> 표 이름과 규칙 어휘다.
     *
     * <p>{@code member} 는 안 넣는다 — 예약도 사용자를 가리키는 낱말이 필요하고,
     * 그 자체로는 쿠폰 전용이 아니다. 넣으면 경계가 아니라 <b>어휘 취향</b>을 강제한다.
     *
     * <p>⚠️ <b>낱말 경계를 안 건다.</b> 처음에 {@code coupons?\b} 로 적었는데
     * {@code _} 가 낱말 문자라 <b>{@code coupon_id}·{@code uk_coupon_member} 를 못 잡았다</b> —
     * 컬럼 이름이 전부 그 모양이라 구멍이 정확히 제일 흔한 자리에 났다.
     * 아래 세 번째 시험이 그 구멍을 드러냈다.
     */
    private static final Pattern COUPON_VOCABULARY = Pattern.compile(
            "FindingType|TargetKey|VerificationFinding|coupon|issuance|campaign_id");

    /**
     * <b>가져갈 수 있는 절반.</b> 이 목록이 곧 다른 도메인에 주는 계약이다.
     *
     * <p>여기 파일을 <b>더하는 것은 자유지만 빼는 것은 결정</b>이다 — 뺀다는 것은
     * 그 타입이 쿠폰 전용이 됐다는 뜻이고, 사전예약 쪽 재사용 목록에서도 사라진다.
     */
    private static final List<String> DOMAIN_FREE = List.of(
            // 실행 이력 — 대사가 "언제 무엇을 판정했나" 를 남기는 자리.
            // PRD 의 KPI 셋이 "대사 결과를 조회하여 확인한다" 이므로 이것이 증적이다.
            "VerificationRun.java",
            "VerificationRunRepository.java",
            // 판정 어휘. PASS/FAIL 과 전수/증분은 도메인이 안 붙는다.
            "VerdictType.java",
            "ScopeType.java",
            "StatsStatus.java",
            // 잔여 집계 형(CY-947). PRD 대사 보고서의 "잔여 불일치 건수" 축이고
            // 지속·신규·해소라는 집합 연산의 결과라 규칙 어휘가 안 든다.
            "ResidualCount.java",
            // 실행 이력을 걷는 포트. 무엇을 걷는지는 어댑터가 안다.
            "CleanupRepository.java",
            // (검출 종류, 대상 키) 쌍 — **String 둘뿐이다.** 어느 도메인이든 쓴다.
            // ⚠️ 처음에 쿠폰 전용으로 잘못 적었다. javadoc 이 campaign_id·coupon_id 를
            //    설명해서 그렇게 읽혔는데, **코드에는 도메인이 한 글자도 없다** —
            //    주석을 걷어내고 세는 규칙(docs/19)이 그것을 드러냈다.
            "FindingKey.java");

    /**
     * <b>경계 아래로 분류한 것과 그 이유.</b> 지우는 것이 아니라 <b>쿠폰 전용이라고
     * 적어 두는</b> 자리다 — 사전예약은 이 자리에 자기 것을 만든다.
     */
    private static final Map<String, String> COUPON_SIDE = Map.ofEntries(
            entry("FindingType.java",
                    "규칙 어휘 V1~V6. javadoc 이 V7 을 새로 만들지 않는다고 의도로 적었다"),
            entry("TargetKey.java",
                    "COUPON:/ISSUANCE: 접두사. 대상 식별자의 모양 자체가 도메인이다"),
            entry("VerificationFinding.java",
                    "검출 한 행. 위 둘을 다 든다"),
            entry("VerificationFindingRepository.java",
                    "검출 포트. FindingType 으로 집계를 낸다"),
            entry("VerificationRuleRepository.java",
                    "규칙 질의. coupons·issuances 를 직접 읽는다"),
            entry("StatsRepository.java",
                    "회차·발급 집계"),
            // ⚠️ **이것은 CY-945 가 만든 빚이다.** 검사 규모라는 축 자체는 도메인 무관인데
            //    (PRD 5단계가 "검사 건수" 를 요구한다) 필드 이름을 issuanceCount ·
            //    historyCount 로 박아서 쿠폰 전용이 됐다. 축→수의 맵이었으면 무관이었다.
            //    지금 고치면 저장 스키마(examined_issuance_count)까지 따라 움직이므로
            //    **여기 적어만 둔다** — 사전예약이 쓸 때 그때 일반화한다.
            entry("DatasetScale.java",
                    "축 이름이 쿠폰이다(CY-945). 일반화하면 스키마가 따라 움직인다"));

    /**
     * <b>어휘는 없는데 재사용도 안 되는 것들.</b> 세 번째 시험이 이 통을 만들게 했다 —
     * 처음에는 통이 둘이었고 <i>"쿠폰 전용이면 쿠폰 낱말을 든다"</i> 를 단언했는데,
     * 이 둘이 그 단언을 깼다. <b>모델이 틀렸던 것이다.</b>
     *
     * <p>재사용 가능성은 <b>낱말이 아니라 개념</b>으로 갈린다. 이 둘은 쿠폰이라는 말을
     * 한 번도 안 쓰지만 <b>이 과제의 검증 방식 자체</b>에 묶여 있다 — 사전예약 대사에는
     * 정답 매니페스트도, 요일·시각 발급 통계도 없다.
     *
     * <p>그래서 이 통에는 <b>어휘 검사를 안 건다.</b> 걸면 없는 낱말을 억지로 넣게 되고,
     * 그것은 검사가 아니라 <b>주석 낭비</b>다. 여기서 지키는 것은 <b>분류가 빠지지 않는
     * 것</b> 하나이고 그것은 {@link #everyTypeIsClassified()} 가 진다.
     */
    private static final Map<String, String> EXERCISE_ONLY = Map.ofEntries(
            entry("ExpectedFindingRepository.java",
                    "오염셋 정답 매니페스트. 사전예약 대사에는 정답이 없다 — "
                            + "운영 데이터가 대상이라 무엇이 맞는지를 시드가 안 알려 준다"),
            entry("HourlyIssued.java",
                    "요일·시각 발급 통계. cy-seed 의 hourly_stats 와 짝이다"),
            entry("DatasetType.java",
                    "CLEAN/CORRUPT. 코드에는 도메인이 없지만 정상셋·오염셋이라는 "
                            + "**이 과제의 검증 방식**이다 — 사전예약 대사는 운영 데이터 하나를 본다"));

    @Test
    @DisplayName("가져갈 수 있는 절반이 쿠폰 어휘에 안 닿는다")
    void theReusableHalfStaysFreeOfCouponVocabulary() throws IOException {
        List<String> violations = new ArrayList<>();
        for (String name : DOMAIN_FREE) {
            Path file = SOURCE_ROOT.resolve(name);
            assertThat(file)
                    .as("목록에 있는데 파일이 없다. 옮겼으면 이 목록도 함께 고쳐야 한다 — "
                            + "안 고치면 이 검사가 조용히 죽는다")
                    .exists();
            for (String line : stripComments(Files.readString(file)).split("\n")) {
                if (COUPON_VOCABULARY.matcher(line).find()) {
                    violations.add(name + " : " + line.strip());
                }
            }
        }

        assertThat(violations)
                .as("""
                        도메인 무관이어야 할 타입이 쿠폰 어휘에 닿았다. 셋 중 하나다 —
                        (1) 정말 필요하면 COUPON_SIDE 로 옮기고 이유를 적어라. 그것은
                            사전예약 재사용 목록에서 그 타입이 빠진다는 뜻이다.
                        (2) 설명만 쿠폰으로 적은 것이면 문구를 도메인 없이 고쳐라 —
                            다음 사람이 그 타입을 쿠폰 전용으로 읽는다.
                        (3) 그냥 실수면 되돌려라.""")
                .isEmpty();
    }

    /**
     * <b>목록이 실제 파일과 같아야 한다.</b> 새 타입이 생겼는데 어느 쪽에도 안 적히면
     * 이 검사는 <b>그 타입을 아예 안 본다</b> — 경계가 조용히 흐려지는 유일한 길이다.
     * {@code OrElseNullBudgetTest} 가 같은 이유로 예산을 전수로 맞춘다.
     */
    @Test
    @DisplayName("모든 타입이 둘 중 한쪽에 분류돼 있다")
    void everyTypeIsClassified() throws IOException {
        List<String> unclassified;
        try (var paths = Files.list(SOURCE_ROOT)) {
            unclassified = paths
                    // **하위 디렉터리는 안 본다.** replay/ 는 이력 되감기 구현이고
                    // 여섯 파일이 전부 도메인에 묶여 있다(docs/19 의 방법으로 셌다) —
                    // 이 경계가 지키려는 것은 그 위의 "실행 이력·판정" 계층이다.
                    // 하위를 섞으면 목록이 스물여섯으로 늘고 그중 열둘이 쿠폰이라,
                    // **가져갈 수 있는 절반**이라는 이 파일의 주제가 흐려진다.
                    .filter(path -> !Files.isDirectory(path))
                    .map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".java"))
                    .filter(name -> !DOMAIN_FREE.contains(name))
                    .filter(name -> !COUPON_SIDE.containsKey(name))
                    .filter(name -> !EXERCISE_ONLY.containsKey(name))
                    .sorted()
                    .toList();
        }

        assertThat(unclassified)
                .as("""
                        새 타입이 어느 쪽에도 안 적혔다. 사전예약이 가져갈 수 있는지 없는지를
                        지금 정해라 — 나중에 정하면 그때는 이미 쓰이고 있다.
                        도메인 무관이면 DOMAIN_FREE, 쿠폰 낱말을 들면 COUPON_SIDE,
                        낱말은 없는데 이 과제 고유면 EXERCISE_ONLY 에 **이유와 함께** 넣는다.""")
                .isEmpty();
    }

    /**
     * <b>{@code COUPON_SIDE} 의 분류가 사실과 맞는지 본다.</b> 거기 넣어 두고 정작 쿠폰
     * 어휘가 하나도 없으면 둘 중 하나다 — 가져갈 수 있는데 안 준다고 적었거나,
     * {@code EXERCISE_ONLY} 로 갔어야 한다.
     *
     * <p><b>이 시험이 두 가지를 잡았다.</b> ① 정규식이 {@code coupon_id} 를 못 잡던
     * 구멍(낱말 경계), ② 통이 둘로는 모자란다는 것 — 어휘 없이 재사용 불가인 타입이
     * 둘 있었다. <b>단언이 틀린 것을 잡은 것이 아니라 모델이 틀린 것을 잡았다.</b>
     */
    @Test
    @DisplayName("쿠폰 전용이라고 적은 것은 실제로 쿠폰 어휘를 든다")
    void theCouponSideActuallyCarriesCouponVocabulary() throws IOException {
        List<String> mislabelled = new ArrayList<>();
        for (String name : COUPON_SIDE.keySet()) {
            Path file = SOURCE_ROOT.resolve(name);
            if (!Files.exists(file)) {
                mislabelled.add(name + " : 파일이 없다");
                continue;
            }
            boolean touches = COUPON_VOCABULARY
                    .matcher(stripComments(Files.readString(file))).find();
            if (!touches) {
                mislabelled.add(name + " : 쿠폰 어휘가 없다 — "
                        + COUPON_SIDE.get(name).toLowerCase(Locale.ROOT));
            }
        }

        assertThat(mislabelled)
                .as("쿠폰 전용이라고 적었는데 실제로는 도메인이 안 붙는다. "
                        + "가져갈 수 있는 것을 못 준다고 적어 둔 셈이니 DOMAIN_FREE 로 옮겨라")
                .isEmpty();
    }
    /**
     * 블록 주석·javadoc·줄 주석을 걷어낸다. {@code docs/19} 의 파이썬과 <b>같은 규칙</b>이다 —
     * 두 곳이 다른 규칙으로 세면 문서의 수와 이 가드의 판정이 갈린다.
     */
    private static String stripComments(String source) {
        return source
                .replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("(?m)//.*$", "");
    }

}
