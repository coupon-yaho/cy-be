// 다른 주제가 그대로 가져갈 수 있는 타입이 무엇인지 못 박습니다.
package com.kafkick.core.verification;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>검증 틀에서 다른 주제가 그대로 가져갈 수 있는 것은 값 타입 다섯뿐이다.</b>
 *
 * <p>사전예약 PRD(`~/Downloads/사전예약 시스템 PRD.pdf`, 저장소 밖 문서)는 <b>정합성 대사
 * 배치</b>를 명시하므로 그쪽에서도 대사는 안 지워진다. 그래서 <i>"무엇을 가져가나"</i> 가
 * 질문이 되고, 이 시험이 그 답을 못 박는다.
 *
 * <h2>⚠️ 처음 답은 틀렸다 — 실행 이력 타입은 못 가져간다</h2>
 *
 * <p>처음에 {@code VerificationRun}·{@code VerificationRunRepository} 를 <i>"그대로 가져갈
 * 수 있다"</i> 로 적었다. <b>틀렸다.</b> 낱말만 세고 <b>타입 참조를 안 봤기 때문</b>이다.
 *
 * <pre>
 * VerificationRun            DatasetType dataset · Long seedRunId · String datasetFingerprint
 * VerificationRunRepository  DatasetScale × 2 · DatasetType × 6
 * </pre>
 *
 * <p>{@code DatasetType}(정상셋/오염셋)과 {@code seedRunId}(시드 실행)는 <b>이 과제의 검증
 * 방식 자체</b>다 — 사전예약 대사는 운영 데이터 하나를 본다. 게다가 {@code dataset} 은
 * {@code uk_run_params(as_of, dataset, scope, attempt)} 로 <b>유일성 키에까지</b> 박혀 있다.
 *
 * <p><b>실행 이력이라는 개념은 간다. 이 타입이 안 갈 뿐이다.</b> 그쪽은 자기 축으로 다시
 * 쓴다 — 그때 이 레코드의 <b>모양</b>(언제·무엇을·몇 번째·판정·시작/종료)이 참고가 된다.
 *
 * <h2>어떻게 세나</h2>
 *
 * <p>두 축을 본다. <b>낱말</b>과 <b>타입 참조</b>다.
 *
 * <ul>
 *   <li><b>낱말</b> — {@code FindingType} 은 같은 패키지라 import 가 안 붙어서 import 만
 *       세면 아무것도 못 잡는다. 대소문자를 안 가린다({@code Coupon}·{@code COUPON_PREFIX}
 *       가 전부 그 모양이다).</li>
 *   <li><b>타입 참조</b> — 아래 두 통에 있는 타입 이름을 그대로 금지어로 쓴다.
 *       <b>이것이 없어서 위 오류가 났다.</b></li>
 * </ul>
 *
 * <p><b>주석은 걷어내고 센다.</b> {@code docs/19} 가 같은 축을 세면서 그 규칙을 정했다 —
 * <i>"설명에 도메인 이름이 나오는 것과 타입·필드·SQL 이 도메인에 묶인 것은 전혀 다른
 * 문제다."</i> ⚠️ 형제 {@code CoreArchitectureTest} 는 <b>반대로</b> 주석까지 본다
 * ({@code VerificationRunRepository} 의 javadoc 이 <i>"주석만으로도 위반이 된다"</i> 고 적어
 * 뒀다) — 두 가드가 주석을 다르게 다루는 것은 <b>의도</b>다. 그쪽은 <i>"core 가 어댑터를
 * 안다"</i> 를 막고, 이쪽은 <i>"가져갈 수 있나"</i> 를 가른다.
 */
class VerificationDomainBoundaryTest {

    private static final Path SOURCE_ROOT = Path.of("src", "main", "java",
            "com", "kafkick", "core", "verification");

    /**
     * <b>쿠폰 도메인의 낱말.</b> 표 이름과 규칙 어휘다.
     *
     * <p>⚠️ <b>낱말 경계를 안 걸고 대소문자도 안 가린다.</b> 처음에 {@code coupons?\b} 로
     * 적었다가 {@code coupon_id}·{@code uk_coupon_member} 를 놓쳤고({@code _} 가 낱말
     * 문자다), 대소문자를 가려서 {@code CouponStateMachine}·{@code IssuanceStatus} 스무 개를
     * 또 놓쳤다 — <b>구멍이 두 번 다 제일 흔한 자리에 났다.</b>
     *
     * <p>{@code member} 는 안 넣는다 — 예약도 사용자를 가리키는 낱말이 필요하다.
     * {@code uk_coupon_member}·{@code DUP_PER_MEMBER} 는 {@code coupon} 쪽으로 잡힌다.
     */
    private static final Pattern COUPON_VOCABULARY = Pattern.compile(
            "coupon|issuance|issued|campaign|stock|grade", Pattern.CASE_INSENSITIVE);

    /** 홑따옴표는 안 본다 — 문자 하나에는 주석 기호가 못 들어간다. */
    private static final Pattern STRING_LITERAL =
            Pattern.compile("\"(?:[^\"\\\\]|\\\\.)*\"");

    /**
     * <b>그대로 가져갈 수 있는 것.</b> 전부 값 타입이고 필드가 원시형·문자열뿐이다.
     *
     * <p><b>검사가 지키는 것은 그보다 좁다</b> — <i>"이 패키지의 비-PORTABLE 타입을 안
     * 문다"</i> 까지다(하위 패키지 포함). {@code LocalDate} 같은 바깥 타입이나 PORTABLE
     * 끼리의 참조는 안 막는다 — 앞엣것은 도메인이 아니고 뒤엣것은 <b>둘 다 가져가므로</b>
     * 재사용을 안 막기 때문이다. <i>"원시형·문자열뿐"</i> 은 <b>목록에 넣는 사람의 판단</b>
     * 이지 이 시험의 단언이 아니다.
     *
     * <p>여기 파일을 <b>더하는 것은 자유지만 빼는 것은 결정</b>이다 — 뺀다는 것은
     * 다른 주제의 재사용 목록에서도 사라진다는 뜻이다.
     */
    private static final List<String> PORTABLE = List.of(
            // PASS/FAIL. 판정이라는 개념에 도메인이 안 붙는다.
            "VerdictType.java",
            // 전수/증분. 대사의 범위 축이고 예약에도 그대로 있다.
            "ScopeType.java",
            // 집계가 완전한가/부분인가/건너뛰었나.
            "StatsStatus.java",
            // 잔여 집계(CY-947). 지속·신규·해소는 집합 연산의 결과다.
            "ResidualCount.java",
            // (검출 종류, 대상 키) 쌍 — String 둘뿐이다.
            // ⚠️ 처음에 쿠폰 전용으로 잘못 적었다. javadoc 이 campaign_id·coupon_id 를
            //    설명해서 그렇게 읽혔는데 코드에는 도메인이 한 글자도 없다.
            "FindingKey.java");

    /**
     * <b>쿠폰 낱말이나 쿠폰 타입을 직접 든다.</b> 규칙 어휘와 그 질의다.
     *
     * <p>둘 중 하나면 된다 — {@code VerificationFindingRepository} 는 낱말이 하나도 없고
     * {@code FindingType} 만 무는데, <b>그 타입이 곧 V1~V6 어휘</b>라 쿠폰 쪽이 맞다.
     * (항진명제를 뺀 세 번째 시험이 그 구분을 강제했다.)
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
            entry("DatasetScale.java",
                    "축은 무관한데(PRD 도 검사 건수를 요구한다) 필드 이름이 "
                            + "issuanceCount·historyCount 다(CY-945). 일반화하면 "
                            + "examined_issuance_count 까지 따라 움직여 그때 갚는다"));

    /**
     * <b>낱말은 없는데 재사용도 안 되는 것들.</b> 재사용 가능성은 <b>낱말이 아니라 개념</b>
     * 으로 갈린다 — 이 통이 그것을 담는다.
     */
    private static final Map<String, String> EXERCISE_ONLY = Map.ofEntries(
            // ⚠️ 이 둘이 처음에 PORTABLE 에 있었다. 타입 참조를 안 봐서 통과했다.
            entry("VerificationRun.java",
                    "DatasetType·seedRunId·datasetFingerprint 를 필드로 든다. "
                            + "dataset 은 uk_run_params 유일성 키에까지 박혀 있다"),
            entry("VerificationRunRepository.java",
                    "시그니처 여덟 자리가 DatasetType·DatasetScale 을 쓴다"),
            entry("DatasetType.java",
                    "CLEAN/CORRUPT. 코드엔 도메인이 없지만 정상셋·오염셋을 병렬로 두는 것이 "
                            + "이 과제의 검증 방식이다 — 사전예약 대사는 운영 데이터 하나를 본다"),
            entry("ExpectedFindingRepository.java",
                    "오염셋 정답 매니페스트. corruptionCountOf 의 계약이 "
                            + "docs/contract.json 의 corruption.matrix 에 묶여 있다"),
            entry("HourlyIssued.java",
                    "요일×시각 히트맵 자체는 무관하지만 DAYS 가 hourly_stats.day_of_week "
                            + "varchar(3) 및 cy-seed 의 config.py 와 글자 단위로 맞춰져 있다"),
            entry("CleanupRepository.java",
                    "메서드 이름이 asof_state·findings 를 든다 — 걷는 대상이 이 과제의 표다"));

    /**
     * <b>재사용 판정에서 뺀 하위 패키지.</b> 목록에 안 적으면
     * {@link #everyTypeIsClassified()} 가 <b>안 보는 것</b>이 조용히 생긴다.
     */
    private static final Map<String, String> SUBPACKAGES = Map.of(
            "replay", "이력 되감기 구현. 일곱 중 여섯이 도메인에 묶인다 "
                    + "(AsOfStateRepository 만 깨끗한데, 그 표의 PK 가 (run_id, coupon_id) 라 "
                    + "낱말만 깨끗하다)",
            "exception", "오류 코드. 낱말은 없지만 검증 규칙의 실패 어휘라 그쪽을 따라간다");

    @Test
    @DisplayName("가져갈 수 있는 타입은 쿠폰 낱말도 이 패키지의 비-PORTABLE 타입도 안 문다")
    void portableTypesCarryNeitherVocabularyNorLocalTypes() throws IOException {
        Pattern localTypes = localTypeReferences();

        List<String> violations = new ArrayList<>();
        for (String name : PORTABLE) {
            Path file = SOURCE_ROOT.resolve(name);
            assertThat(file)
                    .as("목록에 있는데 파일이 없다. 옮겼으면 이 목록도 함께 고쳐야 한다 — "
                            + "안 고치면 이 검사가 조용히 죽는다")
                    .exists();
            String code = stripComments(Files.readString(file));
            for (String line : code.split("\n")) {
                if (COUPON_VOCABULARY.matcher(line).find()) {
                    violations.add(name + " [낱말] " + line.strip());
                }
                if (localTypes.matcher(line).find()) {
                    violations.add(name + " [타입] " + line.strip());
                }
            }
        }

        assertThat(violations)
                .as("""
                        가져갈 수 있다고 적어 둔 타입이 도메인에 닿았다.
                        [낱말] 이면 쿠폰 어휘가 코드에 들어왔다.
                        [타입] 이면 이 과제 전용 타입을 물었다 — **처음에 이 축을 안 봐서
                        VerificationRun 이 DatasetType 을 든 채로 통과했다.**
                        정말 필요하면 EXERCISE_ONLY 로 내리고 이유를 적어라. 그것은 다른
                        주제의 재사용 목록에서 그 타입이 빠진다는 뜻이다.""")
                .isEmpty();
    }

    /**
     * <b>목록이 실제 파일과 같아야 한다.</b> 새 타입이 어느 쪽에도 안 적히면 이 검사는
     * <b>그 타입을 아예 안 본다</b> — 경계가 조용히 흐려지는 유일한 길이다.
     */
    @Test
    @DisplayName("모든 타입과 하위 패키지가 분류돼 있다")
    void everyTypeIsClassified() throws IOException {
        List<String> unclassified = new ArrayList<>();
        // **디렉터리는 깊이 상관없이 본다.** 한때 Files.list 로 한 겹만 봐서
        // replay/ 아래 중첩 패키지가 생기면 아무 분류 없이 통과했다.
        try (var paths = Files.walk(SOURCE_ROOT)) {
            for (Path path : paths.filter(Files::isDirectory).sorted().toList()) {
                if (path.equals(SOURCE_ROOT)) {
                    continue;
                }
                if (!SUBPACKAGES.containsKey(path.getFileName().toString())) {
                    unclassified.add(SOURCE_ROOT.relativize(path) + "/ (하위 패키지)");
                }
            }
        }
        try (var paths = Files.list(SOURCE_ROOT)) {
            for (Path path : paths.sorted().toList()) {
                String name = path.getFileName().toString();
                if (!Files.isDirectory(path) && name.endsWith(".java")
                        && !PORTABLE.contains(name)
                        && !COUPON_SIDE.containsKey(name)
                        && !EXERCISE_ONLY.containsKey(name)) {
                    unclassified.add(name);
                }
            }
        }

        assertThat(unclassified)
                .as("""
                        새 타입이나 하위 패키지가 어느 쪽에도 안 적혔다. 지금 정해라 —
                        나중에 정하면 그때는 이미 쓰이고 있다.
                        그대로 가져갈 수 있으면 PORTABLE, 쿠폰 낱말을 들면 COUPON_SIDE,
                        낱말은 없는데 이 과제 고유면 EXERCISE_ONLY 에 **이유와 함께**.""")
                .isEmpty();
    }

    /**
     * <b>{@code COUPON_SIDE} 의 분류가 사실과 맞는지 본다.</b> 거기 넣어 두고 정작 쿠폰
     * 어휘가 없으면 <b>가져갈 수 있는데 안 준다고 적어 둔 것</b>이거나
     * {@code EXERCISE_ONLY} 로 갔어야 한다.
     *
     * <p><b>자기 타입 이름은 뺀다.</b> 안 빼면 {@code FindingType.java} 가 자기 선언 하나로
     * 통과해 <b>무슨 짓을 해도 안 깨진다</b> — 항진명제다.
     */
    @Test
    @DisplayName("쿠폰 전용이라고 적은 것은 실제로 쿠폰 낱말을 든다")
    void theCouponSideActuallyCarriesCouponVocabulary() throws IOException {
        List<String> mislabelled = new ArrayList<>();
        for (Map.Entry<String, String> each : COUPON_SIDE.entrySet()) {
            Path file = SOURCE_ROOT.resolve(each.getKey());
            if (!Files.exists(file)) {
                mislabelled.add(each.getKey() + " : 파일이 없다");
                continue;
            }
            String own = each.getKey().replace(".java", "");
            String code = stripComments(Files.readString(file)).replace(own, "");
            boolean carriesVocabulary = COUPON_VOCABULARY.matcher(code).find();
            boolean carriesCouponType = couponTypeReferences(own).matcher(code).find();
            if (!carriesVocabulary && !carriesCouponType) {
                mislabelled.add(each.getKey() + " : 쿠폰 낱말도 쿠폰 타입도 없다 — "
                        + each.getValue());
            }
        }

        assertThat(mislabelled)
                .as("쿠폰 전용이라고 적었는데 자기 이름 말고는 도메인이 안 붙는다. "
                        + "가져갈 수 있으면 PORTABLE, 개념이 이 과제 고유면 EXERCISE_ONLY 다")
                .isEmpty();
    }

    /** 세 통 모두 실제 파일을 가리켜야 한다 — 지워진 항목은 영원히 죽은 채로 남는다. */
    @Test
    @DisplayName("분류에 적힌 파일이 전부 실재한다")
    void everyClassifiedFileExists() {
        List<String> missing = new ArrayList<>();
        Set<String> all = new LinkedHashSet<>(PORTABLE);
        all.addAll(COUPON_SIDE.keySet());
        all.addAll(EXERCISE_ONLY.keySet());
        for (String name : all) {
            if (!Files.exists(SOURCE_ROOT.resolve(name))) {
                missing.add(name);
            }
        }

        assertThat(missing)
                .as("분류에 있는데 파일이 없다. 옮기거나 지웠으면 목록도 함께 고쳐라")
                .isEmpty();
    }

    /** 자기 자신을 뺀 쿠폰 타입 이름들. 자기 선언으로 통과하는 항진명제를 막는다. */
    private static Pattern couponTypeReferences(String self) {
        List<String> names = COUPON_SIDE.keySet().stream()
                .map(name -> name.replace(".java", ""))
                .filter(name -> !name.equals(self))
                .toList();
        return Pattern.compile("\\b(" + String.join("|", names) + ")\\b");
    }

    /**
     * <b>한 파일이 두 통에 동시에 있으면 안 된다.</b> 세 통은 <b>서로 다른 재사용 결정</b>
     * 이라, 겹치면 모순인 채로 통과한다 — {@link #everyClassifiedFileExists()} 가 집합으로
     * 합쳐서 보기 때문에 거기서도 안 걸린다.
     */
    @Test
    @DisplayName("한 파일은 정확히 한 통에만 있다")
    void everyTypeIsClassifiedExactlyOnce() {
        List<String> duplicated = new ArrayList<>();
        List<List<String>> buckets = List.of(PORTABLE,
                List.copyOf(COUPON_SIDE.keySet()), List.copyOf(EXERCISE_ONLY.keySet()));
        Set<String> seen = new LinkedHashSet<>();
        for (List<String> bucket : buckets) {
            for (String name : bucket) {
                if (!seen.add(name)) {
                    duplicated.add(name);
                }
            }
        }

        assertThat(duplicated)
                .as("두 통에 같이 적혀 있다. 가져갈 수 있거나 없거나 둘 중 하나다")
                .isEmpty();
    }

    /**
     * <b>{@link #stripComments} 의 전제를 시험으로 든다.</b> 그 정규식은 문자열 리터럴
     * 안의 주석 기호를 <b>진짜 주석으로 오인</b>해 뒤의 코드를 지운다 — 그러면 경계 위반이
     * 조용히 통과한다.
     *
     * <p>어휘 분석기를 넣는 대신 <b>그 전제가 아직 참인지</b>를 잰다. 지금 이 패키지에는
     * 그런 리터럴이 하나도 없고, 생기는 날 여기가 빨개져 그때 파서로 갈아탄다.
     * {@code docs/19} 의 파이썬도 같은 결함을 공유하므로 두 판정이 갈리지는 않는다.
     */
    @Test
    @DisplayName("문자열 리터럴 안에 주석 기호가 없다 — 주석 제거의 전제다")
    void noStringLiteralCarriesCommentMarkers() throws IOException {
        List<String> risky = new ArrayList<>();
        try (var paths = Files.walk(SOURCE_ROOT)) {
            for (Path path : paths.filter(each -> each.toString().endsWith(".java")).toList()) {
                // **원본을 본다 — 걷어낸 것을 보면 안 된다.** 여기서 잡으려는 것이 바로
                // "걷어내기가 문자열을 망가뜨리는 것" 인데, 걷어낸 뒤를 보면 그 문자열이
                // 이미 잘려 있어 정규식에 안 걸린다. 처음에 그렇게 써서 돌연변이가
                // 살아남았다 — 시험이 자기가 막으려는 것을 못 보고 있었다.
                String code = Files.readString(path);
                for (String literal : STRING_LITERAL.matcher(code).results()
                        .map(match -> match.group()).toList()) {
                    if (literal.contains("//") || literal.contains("/*")) {
                        risky.add(SOURCE_ROOT.relativize(path) + " : " + literal);
                    }
                }
            }
        }

        assertThat(risky)
                .as("문자열 안에 주석 기호가 들어왔다. stripComments 가 그것을 주석으로 "
                        + "오인해 뒤의 코드를 통째로 지운다 — 경계 위반이 조용히 통과한다. "
                        + "이 시험이 빨개지면 정규식이 아니라 어휘 분석으로 갈아탈 때다")
                .isEmpty();
    }

    /**
     * <b>이 과제 전용 타입의 이름을 그대로 금지어로 쓴다.</b> 목록에서 만들므로
     * 통을 고치면 금지어가 따라 움직인다 — 두 곳에 적으면 갈린다.
     */
    private static Pattern localTypeReferences() throws IOException {
        List<String> names = new ArrayList<>();
        COUPON_SIDE.keySet().forEach(name -> names.add(name.replace(".java", "")));
        EXERCISE_ONLY.keySet().forEach(name -> names.add(name.replace(".java", "")));
        // **하위 패키지 타입도 넣는다.** 재사용 판정에서 뺀 것들이라 PORTABLE 이 그것을
        // 물면 같은 오염이다 — 한때 최상위 이름만 봐서 이 축이 비어 있었다.
        try (var paths = Files.walk(SOURCE_ROOT)) {
            paths.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.getParent().equals(SOURCE_ROOT))
                    .map(path -> path.getFileName().toString().replace(".java", ""))
                    .forEach(names::add);
        }
        return Pattern.compile("\\b(" + String.join("|", names) + ")\\b");
    }

    /**
     * 블록 주석·javadoc·줄 주석을 걷어낸다. {@code docs/19} 의 파이썬과 같은 규칙이다.
     *
     * <p>⚠️ 줄 주석 안의 {@code /*} 는 블록 정규식이 먼저 돌아 <b>뒤의 코드를 통째로
     * 먹는다.</b> 지금 이 패키지에는 텍스트 블록도, 문자열 안의 주석 기호도 <b>0건</b>이라
     * 안 나지만({@code docs/19} 의 파이썬도 같은 결함을 공유한다), SQL 텍스트 블록에
     * MySQL 힌트 {@code /*+ ... *&#47;} 가 들어오는 날 같은 사고가 난다.
     */
    private static String stripComments(String source) {
        return source
                .replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("(?m)//.*$", "");
    }
}
