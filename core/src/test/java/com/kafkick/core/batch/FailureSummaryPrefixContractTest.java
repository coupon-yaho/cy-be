package com.kafkick.core.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>{@link FailureSummary} 의 접두사 목록이 저장소의 실제 에러코드를 덮는지 본다.</b>
 *
 * <p>목록에 없는 도메인의 코드는 <b>예외 클래스 이름으로 뭉개진다.</b> 그러면 화면은
 * {@code IllegalStateException} 같은 값을 보여 주고, 우리가 정의한 코드가 있었다는 사실
 * 자체가 사라진다 — <b>예외가 안 나므로 아무도 모른다.</b>
 *
 * <h2>왜 기계로 세나</h2>
 *
 * <p>이 목록을 처음 만들 때 사람이 {@code [A-Z]+-} 로 훑었고, <b>밑줄이 들어간 셋을 통째로
 * 빠뜨렸다</b>({@code COUPON_ROUND}·{@code COUPON_TEMPLATE}·{@code RUNTIME_CONFIG}).
 * 리뷰가 잡아 줬는데, 같은 실수가 <b>새 도메인이 생길 때마다</b> 반복될 수 있다.
 * 세는 일은 정규식 하나면 된다.
 *
 * <h2>왜 목록을 없애고 정규식을 넓히지 않나</h2>
 *
 * <p>{@code [A-Z_]+-\\d{3}} 로 넓히면 <b>오탐한다.</b> 이 저장소에 실재하는 것만 꼽아도
 * {@code SHA-256}({@code MessageDigest.getInstance})과 {@code ISO-8859-1} 이 걸린다.
 * {@code find()} 는 첫 매치를 쓰므로, 그 오탐이 뒤에 있는 <b>진짜 코드를 가린다.</b>
 * 그래서 열거를 유지하고 <b>열거가 낡지 않는지를</b> 대신 검사한다.
 */
class FailureSummaryPrefixContractTest {

    /** 저장소 뿌리. core 테스트의 작업 디렉터리가 모듈 안이라 한 칸 올라간다. */
    private static final Path REPO_ROOT = Path.of("..");

    /**
     * {@code "COUPON_ROUND-001"}·{@code "ADMIN-INQUIRY-001"} 처럼 <b>문자열 리터럴로 적힌</b>
     * 에러코드.
     *
     * <p><b>접두사에 하이픈이 여러 개 올 수 있다.</b> 첫 판은 {@code [A-Z][A-Z_]*-} 로 써서
     * {@code ADMIN-COUPON-ROUND-001} 을 <b>아예 수집하지 못했고</b>, 그래서 이 검사가
     * 조용히 통과했다 — <b>세는 쪽이 알아보는 쪽과 같은 맹점</b>을 가지면 검사가 검사를
     * 못 한다. 리뷰가 잡았다.
     */
    private static final Pattern LITERAL_CODE =
            Pattern.compile("\"([A-Z][A-Z_]*(?:-[A-Z][A-Z_]*)*)-\\d{3}\"");

    /**
     * 에러코드가 아닌데 형태가 같은 것들. <b>이것이 정규식을 못 넓히는 이유의 증거다.</b>
     *
     * <p>{@code SHA-256} 은 {@code MessageDigest.getInstance} 인자이고,
     * {@code ISO-8859-1} 은 문자셋 이름이다.
     */
    private static final Set<String> NOT_ERROR_CODES = Set.of("SHA", "ISO");

    @Test
    @DisplayName("저장소의 모든 에러코드 접두사를 FailureSummary 가 알아본다")
    void everyErrorCodePrefixIsRecognised() throws IOException {
        Set<String> missing = new TreeSet<>();
        try (Stream<Path> sources = Files.walk(REPO_ROOT)) {
            List<Path> files = sources
                    .filter(file -> file.toString().endsWith(".java"))
                    .filter(file -> file.toString().replace('\\', '/').contains("/src/main/"))
                    .filter(file -> !file.toString().replace('\\', '/').contains("/build/"))
                    .toList();
            for (Path file : files) {
                Matcher found = LITERAL_CODE.matcher(
                        Files.readString(file, StandardCharsets.UTF_8));
                while (found.find()) {
                    String prefix = found.group(1);
                    if (NOT_ERROR_CODES.contains(prefix)) {
                        continue;
                    }
                    // 실제 코드 하나를 그대로 태워 본다 — 접두사만 비교하면 정규식의
                    // 교대 순서 같은 실수를 못 잡는다.
                    String sample = prefix + "-001";
                    if (!sample.equals(FailureSummary.of(sample + " 무언가 실패했습니다"))) {
                        missing.add(prefix);
                    }
                }
            }
        }

        assertThat(missing)
                .as("이 접두사의 코드는 예외 이름으로 뭉개져 화면에서 사라집니다. "
                        + "FailureSummary.DOMAIN_CODE 에 추가하십시오")
                .isEmpty();
    }

    /** 밑줄이 들어간 접두사가 실제로 살아난다 — 리뷰가 잡은 첫 자리다. */
    @Test
    @DisplayName("밑줄이 들어간 접두사도 알아본다")
    void recognisesPrefixesWithUnderscores() {
        assertThat(FailureSummary.of("COUPON_ROUND-002 회차가 없습니다"))
                .isEqualTo("COUPON_ROUND-002");
        assertThat(FailureSummary.of("RUNTIME_CONFIG-001 값이 없습니다"))
                .isEqualTo("RUNTIME_CONFIG-001");
    }

    /**
     * 하이픈이 여러 개인 접두사 — 리뷰가 잡은 <b>두 번째</b> 자리다.
     *
     * <p>{@code ADMIN} 이 먼저 걸려 {@code ADMIN-INQUIRY-001} 을 못 알아보면, 그 코드는
     * 예외 이름으로 뭉개져 화면에서 사라진다.
     */
    @Test
    @DisplayName("하이픈이 여러 개인 접두사도 알아본다")
    void recognisesPrefixesWithMultipleHyphens() {
        assertThat(FailureSummary.of("ADMIN-INQUIRY-002 조회 실패"))
                .isEqualTo("ADMIN-INQUIRY-002");
        assertThat(FailureSummary.of("ADMIN-COUPON-ROUND-001 회차 데이터 없음"))
                .isEqualTo("ADMIN-COUPON-ROUND-001");
        // 짧은 쪽도 그대로 산다.
        assertThat(FailureSummary.of("ADMIN-003 관측 꺼짐")).isEqualTo("ADMIN-003");
    }

    /**
     * <b>{@code COUPON} 이 {@code COUPON_ROUND} 를 가리지 않는다.</b> 교대에서 짧은 쪽이
     * 먼저 걸리면 {@code COUPON_ROUND-002} 에서 뒤쪽만 남거나 아예 안 잡힌다.
     */
    @Test
    @DisplayName("짧은 접두사가 긴 접두사를 가리지 않는다")
    void theShorterPrefixDoesNotShadowTheLongerOne() {
        assertThat(FailureSummary.of("COUPON-001 발급 실패")).isEqualTo("COUPON-001");
        assertThat(FailureSummary.of("COUPON_TEMPLATE-003 템플릿 없음"))
                .isEqualTo("COUPON_TEMPLATE-003");
    }
}
