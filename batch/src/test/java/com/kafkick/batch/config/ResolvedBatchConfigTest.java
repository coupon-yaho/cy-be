// 실제 설정 파일 두 벌을 Boot 로 로드해 최종 해석값을 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.kafkick.batch.job.ExpireJobConfig;
import com.kafkick.batch.schedule.ExpireScheduler;
import com.kafkick.batch.job.VerifyJobConfig;
import com.kafkick.storage.db.config.HermeticBoot;
import com.kafkick.storage.db.config.ResolvedConfigChecks;
import java.io.IOException;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.scheduling.support.CronExpression;

/**
 * <b>{@link BatchConfigPrecedenceTest} 는 키가 어느 문서에 있는지만 봅니다.</b>
 * "뒤 문서에 있으니 이길 것이다" 는 추론이고, 그 사이에 검증되지 않은 가정이 하나 끼어 있습니다.
 * 두 번째 문서에 {@code spring.config.activate.on-profile} 이 붙어 통째로 비활성화되면
 * 키는 여전히 뒤 문서에 있으므로 그 테스트는 통과하는데 값은 storage.yml 것이 됩니다.
 *
 * <p>여기서는 <b>결과값</b>을 봅니다. 다만 덮개가 전면적이지는 않습니다 —
 * 하드코딩한 네 프로퍼티만 봅니다.
 *
 * <p>읽는 파일은 {@code build.gradle} 의 {@code processTestResources} 가 복사한 두 벌입니다 —
 * {@code resolved/application.yml} 과 {@code resolved/storage.yml}. 원본이 {@code .example} 이라
 * Boot 가 확장자로 파서를 못 고르기 때문이고, 실제 {@code application.yml} 은 gitignore 대상이라
 * 저장소가 가진 유일한 진실이 {@code .example} 이기 때문입니다.
 *
 * <p>{@link HermeticBoot} 로 띄웁니다 — 셸 환경변수가 판정을 바꾸면 이 테스트의 뜻이 없어집니다.
 */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class ResolvedBatchConfigTest {

    private static final String LOCATION = "--spring.config.location=classpath:/resolved/application.yml";

    /**
     * 플레이스홀더 기본값({@code 4})과도 storage.yml 값({@code 10})과도 다른 값.
     *
     * <p>기본값과 같은 값을 주면 인자가 무시돼도 결과가 같아 구분이 안 된다.
     * 이 값이 나오면 <b>뒤 문서가 이겼고 플레이스홀더도 살아 있다</b>가 한 번에 증명된다.
     */
    private static final String POOL_SIZE = "7";

    /** 플레이스홀더 기본값(9092)과 다른 값. 같은 값을 주면 키 경로가 죽어도 구분이 안 된다. */
    private static final String MANAGEMENT_PORT = "9391";

    /** 밀폐가 깨졌는지 보려고 일부러 심는 키. 설정 파일이 이기면 이 값은 안 보인다. */
    private static final String POLLUTED_KEY = "spring.flyway.enabled";

    /**
     * 잡 설정 클래스들이 쓰는 {@code ${...}} 키 <b>집합</b>.
     *
     * <p>개수가 아니라 집합이다. 개수로 세면 {@code MAX_FINDINGS} 가 세 파라미터에 붙어 있어
     * 같은 키가 3 으로 계산되고, {@code @ConfigurationProperties} 로 묶는 정당한 리팩터에
     * <b>거짓 실패</b>하면서 메시지는 "리플렉션이 한쪽을 놓쳤다" 고 원인을 반대로 말한다.
     *
     * <p>집합이면 빠진 키·는 키가 메시지에 그대로 나온다.
     */
    private static final Set<String> EXPECTED_VALUE_KEYS = Set.of(
            "batch.verify.step-timeout-ms",
            "batch.verify.max-findings-per-rule",
            "batch.scheduling.enabled",
            "batch.verify.chunk-size",
            "batch.verify.replay-window-size",
            "batch.expire.chunk-size",
            "batch.expire.step-timeout-ms",
            // @Scheduled 애너테이션 안에 있어 @Value 파라미터 스캔으로는 안 잡히던 키.
            "batch.schedule.expire-cron");

    /**
     * 셸이 아니라 이 JVM 에서 직접 오염시킨다. 밀폐가 깨지면 아래 단언이 이 값을 보고 실패한다.
     *
     * <p>메서드 본문이 아니라 {@code @BeforeEach} 에 두는 이유는, 나중에 테스트를 하나 더
     * 추가하는 사람이 <b>자기 메서드에 자가검증이 빠졌다는 것을 알 방법이 없기 때문</b>이다.
     */
    private String priorValue;

    @BeforeEach
    void pollute() {
        // 지우는 것이 아니라 되돌린다. 같은 포크 JVM 에서 @SpringBootTest 가 함께 도는데,
        // 원래 값이 있었다면 clearProperty 는 복원이 아니라 삭제다.
        priorValue = System.setProperty(POLLUTED_KEY, "true");
    }

    @AfterEach
    void clean() {
        if (priorValue == null) {
            System.clearProperty(POLLUTED_KEY);
        } else {
            System.setProperty(POLLUTED_KEY, priorValue);
        }
    }

    @Test
    @DisplayName("import 뒤 문서의 덮어쓰기가 실제 해석값까지 살아남는다")
    void overridesSurviveIntoTheResolvedEnvironment() {
        try (ConfigurableApplicationContext context = HermeticBoot.run(
                LOCATION,
                // 파일의 spring.main.web-application-type: servlet 이 빌더의 .web(NONE) 을 이긴다.
                "--spring.main.web-application-type=none",
                // 플레이스홀더 기본값에 기대지 않는다.
                "--BATCH_DB_POOL_SIZE=" + POOL_SIZE,
                // 전부 @Value 기본값과 다른 값이다. 같은 값을 주면 키 경로가 죽어 기본값으로
                // 폴백해도 결과가 같아 구분이 안 된다 — 그게 batch.* 가 조용히 죽는 방식이다.
                // 기본값이 false 라 여기서 false 를 주면 키 경로가 죽어도 같은 값이 나온다.
                // 크론은 먼 미래로 밀어 둔다.
                //
                // 이 부팅기(HermeticBoot.EmptyConfig)에는 컴포넌트 스캔도 @EnableScheduling 도
                // 없어서 스케줄러 빈이 애초에 안 만들어진다 — 지금 이 값이 막고 있는 것은
                // 없다. 부팅기를 실제 컨텍스트로 바꾸는 날을 위한 예비 방어다.
                "--BATCH_SCHEDULING_ENABLED=true",
                "--batch.schedule.expire-cron=0 0 0 1 1 *",
                // batch.expire.* 도 기본값과 다른 값으로 준다. 같은 값이면
                // 키 경로가 죽어 폴백해도 결과가 같아 구분이 안 된다.
                "--EXPIRE_CHUNK_SIZE=13",
                "--EXPIRE_STEP_TIMEOUT_MS=2000",
                "--VERIFY_CHUNK_SIZE=7",
                "--VERIFY_MAX_FINDINGS_PER_RULE=9",
                "--VERIFY_STEP_TIMEOUT_MS=1001",
                "--VERIFY_REPLAY_WINDOW_SIZE=11",
                // 읽는 코드가 아직 없다. 키 경로가 살아 있는지만 본다 — 정리 Step 이 들어올 때
                // 그 티켓은 손잡이가 연결돼 있다는 전제 위에서 시작한다.
                "--ASOF_STATE_KEEP_RUNS=3",
                "--BATCH_MANAGEMENT_PORT=" + MANAGEMENT_PORT)) {
            ConfigurableEnvironment environment = context.getEnvironment();

            assertThat(environment.getProperty("spring.flyway.enabled"))
                    .as("storage.yml 이 true 를 준다. true 로 나오면 batch 가 마이그레이션 소유자가 되거나, "
                            + "밀폐가 깨져 시스템 프로퍼티가 설정 파일을 이긴 것이다")
                    .isEqualTo("false");

            assertThat(environment.getProperty("spring.datasource.hikari.maximum-pool-size"))
                    .as("storage.yml 이 10 을 준다. 10 이면 런타임과 배치가 풀을 나눈다는 전제가 깨진다")
                    .isEqualTo(POOL_SIZE);

            assertThat(environment.getProperty("spring.datasource.hikari.pool-name"))
                    .as("미터의 pool 태그를 가르는 값이다. 첫 문서 키가 살아남는지도 함께 본다")
                    .isEqualTo("batch-pool");

            assertThat(environment.getProperty("spring.datasource.url"))
                    .as("storage.yml 이 실제로 import 됐는지 확인한다. "
                            + "import 가 조용히 실패하면 위 단언들이 '덮어쓰기가 이겼다' 가 아니라 "
                            + "'상대가 없었다' 로 통과해 버린다")
                    .contains("rewriteBatchedStatements=true");

            ResolvedConfigChecks.assertReadsResolvedStorage(environment);
            ResolvedConfigChecks.assertEveryPlaceholderResolves(environment);
            assertBatchKeysAreAlive(environment);
            assertManagementSurfaceIsNarrow(environment);
        }
    }

    /**
     * <b>관측 통로가 열려 있고 위험한 문은 닫혀 있는지 파일에서 확인한다.</b>
     *
     * <p>{@code security-audit} 워크플로와 {@code .coderabbit.yaml} 이 같은 것을 보지만
     * 둘 다 <b>문자열 grep</b> 이다. 파일을 옮기거나 키 경로를 오타 내면 grep 은 통과하고
     * 설정은 죽는다 — {@code batch.*} 가 조용히 죽던 방식과 같다.
     *
     * <p>여기서 보는 것은 <b>파일이 무엇을 말하는가</b>이고, 그 말대로 실제로 막히는지는
     * {@link ActuatorExposureTest} 가 서버를 띄워 확인한다. 둘 중 하나만 있으면
     * "적혀 있는데 안 먹는다" 또는 "먹는데 적혀 있지 않다" 를 못 잡는다.
     */
    private void assertManagementSurfaceIsNarrow(ConfigurableEnvironment environment) {
        assertThat(environment.getProperty("management.server.port"))
                .as("업무 포트와 같은 값이면 verify 트리거와 지표가 한 문에 걸린다")
                .isEqualTo(MANAGEMENT_PORT)
                .isNotEqualTo(environment.getProperty("server.port"));

        assertThat(environment.getProperty("management.endpoints.web.exposure.include"))
                .as("포함 여부가 아니라 **정확히 이 셋**이어야 한다. contains 로 두면 "
                        + "누가 하나 더 넣거나 * 로 바꿔도 통과한다 — 그게 이 설정이 무너지는 방식이다")
                .isEqualTo("health,metrics,prometheus");

        assertThat(environment.getProperty("management.endpoints.web.exposure.exclude"))
                .as("include 가 * 로 넓어지는 날의 이중 방어다. 지금 막고 있는 것은 include 다. "
                        + "contains 로 두면 열두 번째 위험 엔드포인트가 늘어도 침묵한다")
                .isEqualTo("env,configprops,beans,heapdump,loggers,threaddump,"
                        + "mappings,scheduledtasks,conditions,flyway,sbom");

        assertThat(environment.getProperty("server.error.include-stacktrace"))
                .as("예외가 응답으로 나가면 내부 구조가 그대로 드러난다")
                .isEqualTo("never");

        assertThat(environment.getProperty("management.endpoint.health.show-details"))
                .as("상세를 켜면 DB 접속 정보와 컴포넌트 구성이 그대로 나간다")
                .isEqualTo("never");
    }

    /**
     * <b>{@code batch.*} 는 storage.yml 과 겹칠 상대가 없어 다른 방식으로 죽는다.</b>
     * 키 경로가 틀리면 {@code @Value} 가 기본값으로 폴백하고 에러도 경고도 없다.
     *
     * <p>그리고 {@code .example} 의 값과 {@code @Value} 의 기본값이 <b>글자까지 같아서</b>,
     * 운영에서 {@code Environment} 를 찍어 봐도 "적힌 값이 떴다" 로 보인다 —
     * 이 티켓을 만든 사고보다 한 단계 더 조용하다. 그래서 기본값과 <b>다른</b> 값으로 확인한다.
     */
    private void assertBatchKeysAreAlive(ConfigurableEnvironment environment) {
        assertThat(environment.getProperty("batch.scheduling.enabled")).isEqualTo("true");
        assertThat(environment.getProperty("batch.expire.chunk-size")).isEqualTo("13");
        assertThat(environment.getProperty("batch.expire.step-timeout-ms")).isEqualTo("2000");
        assertThat(environment.getProperty("batch.verify.chunk-size")).isEqualTo("7");
        assertThat(environment.getProperty("batch.verify.max-findings-per-rule")).isEqualTo("9");
        assertThat(environment.getProperty("batch.verify.step-timeout-ms")).isEqualTo("1001");
        assertThat(environment.getProperty("batch.verify.replay-window-size")).isEqualTo("11");
        assertThat(environment.getProperty("batch.verify.asof-state-keep-runs")).isEqualTo("3");
    }

    @Test
    @DisplayName("크론 다섯이 실제로 파싱된다 — 읽는 코드가 아직 없어서 문법 오류가 안 드러난다")
    void scheduleCronsParse() {
        try (ConfigurableApplicationContext context = HermeticBoot.run(
                LOCATION, "--spring.main.web-application-type=none")) {
            ConfigurableEnvironment environment = context.getEnvironment();

            // @Scheduled 가 배선되는 티켓까지 기다리면 그때 처음 터진다. 파싱만 시켜 두면
            // 문법 오류는 지금 잡히고, 배선 티켓은 크론이 맞다는 전제 위에서 시작한다.
            for (String key : List.of("expire-cron", "verify-incremental-cron", "cleanup-cron",
                    "coupon-open-cron", "coupon-create-cron")) {
                String cron = environment.getProperty("batch.schedule." + key);

                assertThat(cron).as(key + " 가 해석되지 않았다 — 키 경로부터 확인해라").isNotBlank();
                assertThatCode(() -> CronExpression.parse(cron))
                        .as(key + " = " + cron)
                        .doesNotThrowAnyException();
            }
        }
    }

    @Test
    @DisplayName("복사본의 import 경로가 실제로 치환돼 있다")
    void resolvedCopyPointsAtTheResolvedStorage() throws IOException {
        ResolvedConfigChecks.assertCopyPointsAtResolvedStorage(getClass());
    }

    /**
     * <b>키는 두 곳에 적힌다.</b> {@code .example} 의 경로와 {@code @Value} 의 리터럴이고,
     * 위 단언은 앞쪽만 고정한다. 뒤쪽 리터럴을 고치면(단위 명확화로
     * {@code step-timeout-ms} → {@code step-timeout-millis} 같은 리팩터는 흔하다)
     * {@code @Value} 가 기본값으로 폴백하는데, 그 기본값이 {@code .example} 값과
     * <b>글자까지 같아서</b> 아무 데서도 안 드러난다.
     *
     * <p>그래서 Java 쪽 리터럴을 읽어서 대조한다. 한 벌이면 그 클래스의 키가 전부 덮인다.
     */
    @Test
    @DisplayName("잡 설정의 @Value 키가 전부 설정에 실재한다")
    void everyValuePlaceholderResolves() {
        Pattern placeholder = Pattern.compile("\\$\\{([^:}]+)");

        try (ConfigurableApplicationContext context = HermeticBoot.run(
                LOCATION, "--spring.main.web-application-type=none")) {
            ConfigurableEnvironment environment = context.getEnvironment();
            Set<String> seen = new LinkedHashSet<>();

            // 생성자도 훑는다. step-timeout-ms 가 생성자 파라미터라 메서드만 보면 통째로 빠진다 —
            // 실제로 그 상태에서 돌연변이가 안 잡혔다.
            // 잡 설정 클래스를 전부 훑는다. 한 클래스만 보면 새 잡이 들어올 때
            // 그 잡의 키는 아무도 안 본다 — 이 테스트가 막으려던 폴백이 거기서 그대로 난다.
            //
            // 스케줄러도 훑는다. 크론 키는 @Value 가 아니라 @Scheduled 애너테이션 안에 있어서
            // 파라미터만 보면 통째로 빠진다 — 그리고 그 기본값이 .example 값과 글자까지 같아
            // 키 경로를 오타 내도 동작이 같고 로그도 없다. 이 테스트가 막으려던 그 모양이다.
            List<Executable> declared = new ArrayList<>();
            for (Class<?> config : List.of(
                    VerifyJobConfig.class, ExpireJobConfig.class, ExpireScheduler.class)) {
                declared.addAll(List.of(config.getDeclaredConstructors()));
                declared.addAll(List.of(config.getDeclaredMethods()));
            }

            for (Executable executable : declared) {
                // @Scheduled(cron = "${...}") 처럼 애너테이션 값에 박힌 키
                if (executable instanceof Method method) {
                    Scheduled scheduled = method.getAnnotation(Scheduled.class);
                    if (scheduled != null && !scheduled.cron().isBlank()) {
                        Matcher matcher = placeholder.matcher(scheduled.cron());
                        while (matcher.find()) {
                            String key = matcher.group(1);
                            assertThat(environment.containsProperty(key))
                                    .as(executable.getName() + " 의 @Scheduled cron " + key
                                            + " 가 .example 에 없다 — 기본값으로 조용히 폴백한다")
                                    .isTrue();
                            seen.add(key);
                        }
                    }
                }
                for (Parameter parameter : executable.getParameters()) {
                    Value value = parameter.getAnnotation(Value.class);
                    if (value == null) {
                        continue;
                    }
                    Matcher matcher = placeholder.matcher(value.value());
                    while (matcher.find()) {
                        String key = matcher.group(1);
                        assertThat(environment.containsProperty(key))
                                .as(executable.getName() + " 의 " + value.value()
                                        + " 가 .example 에 없다 — 기본값으로 조용히 폴백한다")
                                .isTrue();
                        seen.add(key);
                    }
                }
            }

            assertThat(seen)
                    .as("잡 설정의 @Value 키 집합이 달라졌다. 빠진 것이 있으면 "
                            + "생성자·메서드 중 한쪽을 못 훑고 있는 것이고, 늘어난 것이 있으면 "
                            + "여기에 추가해라")
                    .isEqualTo(EXPECTED_VALUE_KEYS);
        }
    }
}
