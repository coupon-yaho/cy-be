// 재시도 최악 총합이 문서의 수와 같은지, 그리고 그 수의 원본과 어긋나지 않는지 봅니다.
package com.kafkick.core.notification.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>{@link NotificationRetryBackOffProperties} 의 javadoc 이 실은 수를 지키는 것이
 * 아무것도 없었다.</b>
 *
 * <p>그 문단은 설계 교환을 정당화한다 — <i>"늦게 가는 것보다 한꺼번에 몰려 다시 실패하는
 * 것이 나쁘다"</i>. 근거가 <b>최악 85.2초</b>이고, 그 값이 {@code cap} 과 실패 상한에서
 * 나온다. 둘 중 하나만 움직여도 문서가 조용히 거짓이 된다.
 *
 * <p><b>{@link RetryBudget} 은 아직 프로덕션에서 부르는 곳이 없다.</b> 그래서 지금 이
 * 저장소를 실제로 지키는 것은 <b>이 시험</b>이다.
 */
class RetryBudgetTest {

    /**
     * <b>모듈 안에서 돈다.</b> core 테스트의 작업 디렉터리가 모듈 루트라 저장소 뿌리는
     * 한 칸 위다 — 형제 {@code FailureSummaryPrefixContractTest} 와 같은 이유·같은 모양.
     */
    private static final Path REPO_ROOT = Path.of("..");

    /**
     * <b>운영 기본값을 그 객체에서 읽는다 — 리터럴로 베끼지 않는다.</b>
     *
     * <p>한때 {@code 200ms}·{@code 20s} 를 이 파일에 적어 뒀는데, 그러면 <b>이 시험이
     * 막겠다는 그 결함을 자기가 갖는다</b> — 운영 기본값 {@code cap} 을 60초로 바꿔도
     * 자기 사본으로 85.2초를 계산해 <b>통과했다</b>(실측). 문서가 거짓이 되는 그 변경이
     * 정확히 안 잡히는 상태였다.
     */
    private static final NotificationRetryBackOffProperties DEFAULTS =
            new NotificationRetryBackOffProperties();

    /** 문서가 근거로 드는 실패 상한. 아래 시험이 이 값을 실제 원본과 맞댄다. */
    private static final int DOCUMENTED_LIMIT = 10;

    private final RetryBudget budget =
            new RetryBudget(new FullJitterBackOff(DEFAULTS.getBase(), DEFAULTS.getCap()));

    /**
     * <b>{@code 10 × cap} 이 아니다.</b> 앞쪽 회차는 아직 상한에 안 닿는다 —
     * 그 오해가 실제로 문서에 있었고 Qodo 리뷰가 잡았다.
     *
     * <p>회차별 상한 자체는 {@code FullJitterBackOffTest} 가 이미 못 박는다. 여기서 재는
     * 것은 <b>그 합</b>이다.
     */
    @Test
    @DisplayName("기본값의 최악 누적 대기는 문서가 적은 85.2초다")
    void matchesTheDocumentedWorstCase() {
        assertThat(budget.worstCaseTotalWait(DOCUMENTED_LIMIT))
                .as("이 값이 바뀌면 NotificationRetryBackOffProperties 의 설계 근거가 "
                        + "거짓이 된다. 문서와 함께 고쳐야 한다")
                .isEqualTo(Duration.ofMillis(85_200));
    }

    /**
     * <b>{@code cap} 을 올려도 선형으로 안 는다.</b> 상한에 닿는 회차가 뒤로 밀릴 뿐이다 —
     * 60초로 올리면 <b>1.90배</b>(162초)다. 한때 이 자리에 "세 배" 라고 적었는데,
     * <b>{@code 10 × cap} 오해와 정확히 같은 종류의 착오</b>였다.
     */
    @Test
    @DisplayName("cap 을 세 배로 올려도 총합은 세 배가 아니다")
    void doesNotScaleLinearlyWithCap() {
        RetryBudget tripled = new RetryBudget(
                new FullJitterBackOff(DEFAULTS.getBase(), DEFAULTS.getCap().multipliedBy(3)));

        assertThat(DEFAULTS.getCap())
                .as("아래 162초는 cap 20초를 세 배로 올린 값에서 나온다")
                .isEqualTo(Duration.ofSeconds(20));
        assertThat(tripled.worstCaseTotalWait(DOCUMENTED_LIMIT))
                .isEqualTo(Duration.ofMillis(162_000));
    }

    /**
     * <b>기다리는 횟수는 상한보다 하나 적다.</b> 마지막 실패는 그 자리에서 종착으로 보내므로
     * 그 지연이 안 쓰인다 — 하나 더 세면 이 예산이 실제보다 커져, 마감 검사가 정상 구성을
     * 거절한다.
     */
    @Test
    @DisplayName("대기 횟수는 상한보다 하나 적다")
    void waitsOneFewerTimeThanTheLimit() {
        assertThat(budget.worstCaseTotalWait(1))
                .as("상한이 1 이면 첫 실패가 곧 종착이라 기다릴 일이 없다")
                .isZero();
        assertThat(budget.worstCaseTotalWait(2)).isEqualTo(Duration.ofMillis(400));
        assertThat(budget.worstCaseTotalWait(3)).isEqualTo(Duration.ofMillis(1_200));
    }

    /**
     * <b>시도 비용까지 더해야 마감과 비교할 수 있다.</b> 대기만 세면 정확히 그만큼 모자라고,
     * <b>모자란 예산은 사고를 통과시킨다.</b>
     */
    @Test
    @DisplayName("총합은 대기에 시도마다의 비용을 더한 값이다")
    void addsTheCostOfEveryAttempt() {
        Duration perAttempt = Duration.ofSeconds(3);

        assertThat(budget.worstCaseTotal(DOCUMENTED_LIMIT, perAttempt))
                .as("시도는 매번 일어난다 — 대기보다 한 번 많다")
                .isEqualTo(Duration.ofMillis(85_200).plus(perAttempt.multipliedBy(10)));
    }

    /** 경계에서 갈린다. {@code <} 를 {@code <=} 로 바꾸는 것이 여기서 잡힌다. */
    @Test
    @DisplayName("마감이 총합보다 커야 든다 — 같으면 안 든다")
    void judgesAtTheBoundary() {
        Duration perAttempt = Duration.ofSeconds(3);        // 총합 115.2초
        Duration total = Duration.ofMillis(115_200);

        assertThat(budget.fitsWithin(DOCUMENTED_LIMIT, perAttempt, total.plusMillis(1))).isTrue();
        assertThat(budget.fitsWithin(DOCUMENTED_LIMIT, perAttempt, total))
                .as("딱 맞는 것은 안 드는 것으로 본다 — 그 사이에 다른 일이 하나도 없어야 한다")
                .isFalse();
        assertThat(budget.fitsWithin(DOCUMENTED_LIMIT, perAttempt, Duration.ofSeconds(-1)))
                .as("이미 지난 마감에는 아무 일정도 못 든다")
                .isFalse();
    }

    /**
     * <b>오버플로가 fail-open 이라 막는다.</b> {@code long} 누적이 감기면 음수가 되고,
     * {@code fitsWithin} 이 그것을 마감보다 작다고 읽어 <b>일정이 우주적으로 길 때 정확히
     * 그때만</b> 통과시킨다 — 상한이 없는 것보다 나쁘다.
     */
    @Test
    @DisplayName("셀 수 없이 큰 상한은 거절한다 — 감기면 통과시키기 때문이다")
    void rejectsALimitThatWouldOverflow() {
        assertThatThrownBy(() -> budget.worstCaseTotalWait(Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(RetryBudget.MAX_FAILURE_COUNT_LIMIT));
    }

    @Test
    @DisplayName("말이 안 되는 입력은 전부 거절한다")
    void rejectsNonsenseInput() {
        assertThatThrownBy(() -> budget.worstCaseTotalWait(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> budget.worstCaseTotalWait(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryBudget(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> budget.worstCaseTotal(10, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> budget.worstCaseTotal(10, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> budget.worstCaseTotal(
                10, RetryBudget.MAX_PER_ATTEMPT_COST.plusDays(1)))
                .as("기동 검사에서 부를 때 ArithmeticException 이 나가면 검사가 아니라 "
                        + "기동이 끊긴다 — 계산 전에 돌려보낸다")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> budget.fitsWithin(10, Duration.ZERO, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>상한 10 의 원본은 이 저장소가 스스로 지목해 뒀다.</b>
     * {@code NotificationOutboxRepositoryImpl} 의 상수 javadoc 이 <i>"여기 하나에만 적는다 —
     * 두 벌로 두면 한쪽만 고쳐질 때 경로에 따라 종착 시점이 달라진다"</i> 고 선언한다.
     * 런타임에 실제로 종착을 결정하는 것도 그것이다.
     *
     * <p><b>DB 의 CHECK 제약이 아니다.</b> 한때 그렇게 적었는데, 그러면 <b>위험한 방향을
     * 못 본다</b> — 상한을 10 에서 5 로 <b>내리면</b> CHECK(0~10)는 그대로 통과하고 최악
     * 대기만 85.2초에서 6.4초로 바뀐다. 올리는 쪽만 DB 가 잡는다.
     *
     * <p>Gradle 입력은 손댈 것이 없다. {@code core/build.gradle} 의
     * {@code repositoryMainSources} 가 저장소의 {@code src/main} 자바를 <b>이미</b> 걸고 있고,
     * 이 파일이 그 안이다.
     */
    @Test
    @DisplayName("문서가 쓰는 상한이 종착을 결정하는 상수와 같다")
    void limitMatchesTheConstantThatEndsRetrying() throws IOException {
        Path source = REPO_ROOT.resolve("storage/src/main/java/com/kafkick/storage/db/"
                + "notification/repository/NotificationOutboxRepositoryImpl.java");
        assertThat(source).exists();

        Matcher constant = Pattern.compile("DEAD_AFTER_FAILURES\\s*=\\s*(\\d+)\\s*;")
                .matcher(Files.readString(source));

        assertThat(constant.find())
                .as("상수를 못 찾았다 — 이름이 바뀌었으면 이 시험이 먼저 알아야 한다")
                .isTrue();
        assertThat(Integer.parseInt(constant.group(1)))
                .as("이 수가 바뀌면 최악 대기가 바뀐다. 위 85.2초와 "
                        + "NotificationRetryBackOffProperties 의 문단을 함께 고쳐야 한다")
                .isEqualTo(DOCUMENTED_LIMIT);
    }
}
