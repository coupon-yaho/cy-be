// 배포된 잡마다 시체 임계 가드가 그 잡의 Step 데드라인을 실제로 보는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>잡이 넷째로 늘면 시체 임계 가드가 그 잡을 안 본다.</b>
 *
 * <p>{@link RunningJobProbe} 생성자는 잡별 Step 데드라인을 <b>인자로 하나씩</b> 받아
 * {@code batch.stuck-job-after-ms} 가 그 최댓값보다 큰지 본다. 그 자리 주석이
 * <i>"새 잡이 생길 때마다 여기 인자가 하나 는다 — 그것이 이 가드의 값이다"</i> 인데,
 * <b>안 늘려도 아무 일도 안 일어난다.</b> 기동은 성공하고, 새 잡의 가장 긴 Step 이
 * 임계보다 길어도 검증을 못 받는다 — 그러면 <b>살아 있는 실행이 시체로 판정되고
 * 운영자가 그것을 걷어낸다.</b>
 *
 * <p>{@code GET /api/v1/admin/batch/runs/stuck} 이 전 잡을 한 번에 내면서 그 구멍이
 * 커졌다(CY-938). 잡별 API 는 보정된 셋에만 열려 있었지만 전 잡 조회는 <b>보정 안 된
 * 잡까지 같은 임계로 판정해서</b> 내놓는다.
 *
 * <p>{@link RunningJobProbeSettingsTest} 는 이것을 못 잡는다 — 그쪽은 <b>주어진 팔들</b>
 * 사이의 관계를 보고, 팔이 <b>모자란</b> 것은 못 본다.
 *
 * <h2>왜 컨텍스트를 띄우나</h2>
 *
 * <p>처음엔 소스에서 {@code public Job xxx(} 를 정규식으로 걷었다. <b>세 군데가 틀렸다.</b>
 * ① 빈 이름은 메서드 이름이 아니라 {@code JobBuilder} 에 넘긴 문자열이고 둘이 같다는
 * 보장이 없다 — {@code Job reportJob()} 이 {@code new JobBuilder("dailyReport", ...)} 를
 * 돌려주면 조회는 {@code dailyReport} 로 도는데 정규식은 {@code report} 를 요구한다.
 * ② {@code @Bean} 메서드는 {@code public} 이 아니어도 등록되므로 <b>조용히 빠진다</b> —
 * 이 클래스가 막으려는 실패 모드가 정확히 그 <b>누락</b>이다.
 * ③ {@code Job} 을 돌려주는 헬퍼 메서드를 잡으로 오해한다.
 *
 * <p>그래서 이름을 <b>컨텍스트에서 받는다.</b> {@code Job::getName} 은 조회가 실제로 쓰는
 * 그 이름이다({@code BatchRunMetrics}·{@code BatchHistoryController} 도 같은 자리를 쓴다).
 *
 * <p><b>{@code BatchStuckApiTest} 와 {@code properties}·{@code @Import} 를 똑같이 둔다</b> —
 * {@code MergedContextConfiguration} 이 같아야 컨텍스트와 컨테이너가 재사용된다.
 *
 * <p>프로브 <b>소스</b>를 읽는 쪽은 그대로다. 설정 키는 런타임 객체에 안 남는다.
 * 이 스캔은 {@code batch/build.gradle} 의 {@code scannedJavaSources} 가 이미 입력으로
 * 걸고 있다(뿌리부터 {@code **}{@code /src/**}{@code /*.java}). 새로 선언하지 말 것.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000",
        "server.port=0",
        "management.server.port=0"
})
@Import(MySqlContainerConfig.class)
class JobCalibrationContractTest {

    private static final Path PROBE =
            Path.of("src/main/java/com/kafkick/batch/config/RunningJobProbe.java");

    /**
     * <b>키를 kebab 으로 받는다.</b> {@code @Value} 에는 relaxed binding 이 없어
     * ({@code @ConfigurationProperties} 기능이다) {@code batch.dataFix.step-timeout-ms} 로
     * 적고 yml 에 {@code batch.data-fix.step-timeout-ms} 를 넣으면 <b>기본값이 조용히
     * 쓰인다</b> — 가드가 실제 값이 아니라 기본값으로 임계를 검증하고 통과한다.
     * 이 저장소의 키는 전부 kebab 이다({@code batch.stuck-job-after-ms}).
     */
    private static final Pattern DEADLINE_ARG = Pattern.compile(
            "@Value\\(\"\\$\\{batch\\.([a-z0-9-]+)\\.step-timeout-ms:[^}]*}\"\\)\\s*long\\s+(\\w+)");

    @Autowired
    private List<Job> jobs;

    @Test
    @DisplayName("배포된 잡마다 프로브 생성자에 Step 데드라인이 하나씩 있다")
    void everyDeployedJobHasAStepDeadline() throws IOException {
        List<String> expected = jobs.stream()
                .map(Job::getName)
                .map(JobCalibrationContractTest::deadlineKeyPrefix)
                .sorted()
                .toList();

        List<String> calibrated = deadlineArgs().map(match -> match.group(1)).sorted().toList();

        assertThat(calibrated)
                .as("잡을 더했으면 RunningJobProbe 생성자에 batch.<잡>.step-timeout-ms 를 "
                        + "인자로 더한다 — 안 더하면 그 잡은 시체 판정을 검증 없이 받는다. "
                        + "잡을 지웠으면 그 팔도 지운다 — 남은 팔은 없는 잡의 값으로 임계를 "
                        + "묶는다")
                .containsExactlyElementsOf(expected);
    }

    /**
     * <b>인자를 더하는 것만으로는 아무것도 안 지킨다.</b> 받아만 놓고 비교식에 안 넣으면
     * 그 잡의 데드라인은 {@code longestSilentStep} 에 <b>안 들어가고</b>, 임계는 나머지
     * 잡들만 보고 통과한다 — 앞 시험은 초록이고 기동도 성공한다. 자바가 안 잡아 준다:
     * 안 쓰는 생성자 파라미터는 경고가 아니고, 이 저장소에는 {@code -Werror} 도
     * checkstyle 도 없다.
     *
     * <p><b>그래서 소스를 읽지 않고 생성자를 실제로 태운다.</b> 데드라인 인자를 하나씩
     * 임계 위로 올려 보고, 그때마다 기동이 <b>거절되는지</b>를 본다.
     *
     * <p>처음엔 이것을 텍스트로 쟀다 — <i>"인자 이름이 {@code longestSilentStep} 이 있는
     * 줄에 나오는가"</i>. <b>돌연변이 다섯을 태웠더니 셋이 통과했다.</b>
     * ① 비교식을 지우고 인자 이름을 <b>주석에</b> 남기면 통과한다.
     * ② {@code verify} 옆에 {@code reverify} 가 생기면
     * {@code "reverifyStepTimeoutMs".contains("verifyStepTimeoutMs")} 라 통과한다.
     * ③ 반대로 {@code Math.max} 로 두 줄에 나눠 쓰는 <b>정상 리팩터링이 빨개진다</b> —
     * 그러면 다음 사람이 테스트가 이상하다며 코드를 되돌린다.
     * 셋 다 <b>어떻게 썼는가</b>를 봐서 생긴 문제다. 여기서 보는 것은 <b>무엇이
     * 일어나는가</b> 하나다.
     *
     * <p>{@code RunningJobProbeSettingsTest} 가 손으로 하는 일과 같은데, 팔을 <b>세 개로
     * 적어 두지 않고</b> 생성자에서 읽어 만든다 — 잡이 넷째로 늘면 팔도 저절로 는다.
     */
    @Test
    @DisplayName("데드라인 인자를 하나씩 임계 위로 올리면 전부 기동을 막는다")
    void everyDeadlineArgumentGuardsTheThreshold() throws Exception {
        Constructor<?> constructor = probeConstructor();
        List<Integer> deadlineSlots = deadlineSlots(constructor);
        int stuckSlot = stuckAfterSlot(constructor);

        assertThat(deadlineSlots)
                .as("생성자에서 batch.<잡>.step-timeout-ms 인자를 하나도 못 찾았다 — "
                        + "스캔이 깨졌다")
                .isNotEmpty();

        for (int slot : deadlineSlots) {
            Object[] args = new Object[constructor.getParameterCount()];
            args[0] = null;                       // JobRepository. 생성자 검사는 안 쓴다.
            for (int other : deadlineSlots) {
                args[other] = 1L;                 // 나머지는 임계 아래로 눕힌다.
            }
            args[stuckSlot] = STUCK_AFTER_MS;
            args[slot] = STUCK_AFTER_MS + 1;      // 이 팔 하나만 임계를 넘긴다.

            String key = deadlineKey(constructor, slot);
            assertThatThrownBy(() -> newProbe(constructor, args))
                    .as("%s 만 임계를 넘겼는데 기동이 됐다 — 그 잡의 Step 데드라인이 "
                            + "longestSilentStep 비교에 안 들어간다. 인자를 받는 것만으로는 "
                            + "아무것도 안 지킨다", key)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(key);
        }
    }

    /** 임계 검사만 태운다. 이 값 자체에는 의미가 없다 — 위아래를 가를 기준일 뿐이다. */
    private static final long STUCK_AFTER_MS = 1_800_000L;

    /** {@code RunningJobProbe} 는 생성자가 하나다. 둘이 되면 이 시험이 먼저 깨져야 한다. */
    private static Constructor<?> probeConstructor() {
        Constructor<?>[] constructors = RunningJobProbe.class.getDeclaredConstructors();
        assertThat(constructors)
                .as("생성자가 하나여야 어느 것을 태울지 고를 필요가 없다")
                .hasSize(1);
        return constructors[0];
    }

    private static List<Integer> deadlineSlots(Constructor<?> constructor) {
        return java.util.stream.IntStream.range(0, constructor.getParameterCount())
                .filter(slot -> valueKey(constructor, slot).contains(".step-timeout-ms"))
                .boxed()
                .toList();
    }

    private static int stuckAfterSlot(Constructor<?> constructor) {
        return java.util.stream.IntStream.range(0, constructor.getParameterCount())
                .filter(slot -> valueKey(constructor, slot).contains("stuck-job-after-ms"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "batch.stuck-job-after-ms 인자를 못 찾았다 — 임계를 못 세운다"));
    }

    /** {@code ${batch.verify.step-timeout-ms:600000}} → {@code batch.verify.step-timeout-ms} */
    private static String deadlineKey(Constructor<?> constructor, int slot) {
        String key = valueKey(constructor, slot);
        int defaulted = key.indexOf(':');
        return defaulted < 0 ? key : key.substring(0, defaulted);
    }

    /** {@code @Value} 가 없는 자리(예: {@code JobRepository})는 빈 문자열이다. */
    private static String valueKey(Constructor<?> constructor, int slot) {
        for (Annotation annotation : constructor.getParameterAnnotations()[slot]) {
            if (annotation instanceof Value value) {
                return value.value().replace("${", "").replace("}", "");
            }
        }
        return "";
    }

    /** 리플렉션이 감싼 예외를 벗긴다 — 단언이 보려는 것은 생성자가 던진 그것이다. */
    private static Object newProbe(Constructor<?> constructor, Object[] args) throws Throwable {
        constructor.setAccessible(true);
        try {
            return constructor.newInstance(args);
        } catch (InvocationTargetException wrapped) {
            throw wrapped.getCause();
        }
    }

    private static java.util.stream.Stream<MatchResult> deadlineArgs() throws IOException {
        return DEADLINE_ARG.matcher(Files.readString(PROBE)).results();
    }

    /**
     * {@code expireJob} → {@code expire}, {@code dataFixJob} → {@code data-fix}.
     * 뒤 규칙이 필요한 이유는 {@link #DEADLINE_ARG} 에 적었다.
     */
    private static String deadlineKeyPrefix(String jobName) {
        String base = jobName.endsWith("Job")
                ? jobName.substring(0, jobName.length() - "Job".length())
                : jobName;
        return base.replaceAll("(?<=[a-z0-9])(?=[A-Z])", "-").toLowerCase(Locale.ROOT);
    }
}
