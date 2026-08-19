package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

import javax.sql.DataSource;

import com.kafkick.storage.db.MySqlContainerConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.awaitility.Awaitility;
import org.apache.tomcat.util.modeler.Registry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 자동 계측이 실제로 등록되는지 본다. 우리가 만드는 미터가 아니라 <b>공짜로 얻는</b> 미터들이고,
 * 그래서 조용히 사라져도 아무도 모른다 — 등록이 깨졌을 때 빨간불이 되는 게 이 테스트의 전부다.
 *
 * <p>이름은 전부 {@link MeterNames} 를 통해 본다. 여기서 문자열을 다시 적으면 상수가 바뀌어도
 * 통과하는 테스트가 된다.
 *
 * <p>MySQL 컨테이너가 필요하다 — {@code hikaricp.*} 는 풀이 실제로 만들어져야 등록된다.
 *
 * <p>설정은 커밋되는 {@code observation.yml.example} 을 고정 경로에 복사해서 쓴다. 실제로
 * 로드되는 {@code observation.yml} 은 gitignore 대상이라 신규 클론에 없다. 경로가 고정
 * 문자열인 이유는 {@code @SpringBootTest} 의 properties 가 애노테이션 상수여야 해서다.
 */
class AutoInstrumentedMetersTest {

    /** {@link #reviveTomcatMbeanRegistry()} 가 덮어쓰기 전의 값. @AfterAll 에서 되돌린다. */
    private static Object previousRegistry;

    /** 컨텍스트가 뜨기 전에 여기로 복사된다. build 아래라 clean 으로 지워진다. */
    private static final Path STAGED_CONFIG = Path.of("build/obs3-config/observation.yml");

    /** 커밋되지 않는 application.yml 을 타지 않으려고 기본 로딩을 통째로 대체한다. */
    private static final String CONFIG_LOCATION =
            "spring.config.location=file:build/obs3-config/observation.yml";

    /**
     * 커밋되는 {@code .example} 을 실제 로드되는 이름으로 복사한다. 값을 프로퍼티로 주입하면
     * ConfigData 로딩 경로를 건너뛰어, 정작 그 파일에서만 나는 오류를 못 잡는다.
     */
    @BeforeAll
    static void stageObservationYaml() throws Exception {
        Files.createDirectories(STAGED_CONFIG.getParent());
        try (InputStream source =
                     new ClassPathResource("observation.yml.example").getInputStream()) {
            Files.copy(source, STAGED_CONFIG, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Tomcat 의 MBean Registry 를 되살린다. 없으면 이 클래스는 <b>실행 순서</b>에 따라 깨진다 —
     * {@code mbeanregistry} 기본값이 false 라, 그 상태로 뜬 다른 웹 테스트가
     * {@code Registry.disableRegistry()} 를 부르면 <b>JVM 전체</b>의 Registry 가 죽고, 그 뒤에
     * 뜨는 우리 Tomcat 은 스위치를 켜도 소용이 없다.
     *
     * <p>{@code disableRegistry()} 는 static 필드에 죽은 인스턴스를 꽂아서 되돌릴 공개 API 가
     * 없다. 그래서 필드를 직접 비운다. 프레임워크 내부에 손대는 대가로, Tomcat 이 이 필드
     * 이름을 바꾸면 {@code NoSuchFieldException} 으로 즉시 깨진다 — 조용히 무력화되지는 않는다.
     */
    @BeforeAll
    static void reviveTomcatMbeanRegistry() throws Exception {
        previousRegistry = registryField().get(null);
        registryField().set(null, null);
    }

    /** 만진 전역 상태를 되돌린다 — 켜 놓고 나가면 뒤 테스트가 그걸 물려받는다. */
    @AfterAll
    static void restoreTomcatMbeanRegistry() throws Exception {
        registryField().set(null, previousRegistry);
    }

    private static java.lang.reflect.Field registryField() throws Exception {
        java.lang.reflect.Field field = Registry.class.getDeclaredField("registry");
        field.setAccessible(true);
        return field;
    }

    @SpringBootTest(
            classes = TestApp.class,
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = CONFIG_LOCATION)
    @Import(MySqlContainerConfig.class)
    @Nested
    class Registration {

        @Autowired
        MeterRegistry registry;

        @Autowired
        DataSource dataSource;

        @LocalServerPort
        int port;

        @Test
        @DisplayName("hikaricp · jvm.memory · process.cpu · tomcat.threads 가 전부 등록된다")
        void allFourFamiliesAreRegistered() throws Exception {
            // 풀은 첫 커넥션에서 만들어지고, hikaricp.* 미터도 그때 등록된다.
            dataSource.getConnection().close();
            MeterValueReader reader = new MeterValueReader(registry);

            assertThat(reader.exists(MeterNames.HIKARI_ACTIVE)).as("hikaricp").isTrue();
            assertThat(reader.exists(MeterNames.HIKARI_PENDING)).as("hikaricp").isTrue();
            assertThat(reader.exists(MeterNames.JVM_MEMORY_USED)).as("jvm.memory").isTrue();
            assertThat(reader.exists(MeterNames.CPU_USAGE)).as("process.cpu").isTrue();
            assertThat(reader.exists(MeterNames.TOMCAT_BUSY)).as("tomcat.threads").isTrue();
        }

        /**
         * 완료 조건 "0.99 값 확인" 이 실제로 걸리는 지점이다.
         *
         * <p>백분위가 어디로 나오는지는 <b>레지스트리에 따라 다르다</b>(실측).
         * {@code SimpleMeterRegistry} 는 {@code <이름>.percentile} 게이지를 phi 태그와 함께
         * 만들지만 {@code PrometheusMeterRegistry} 는 그걸 만들지 않고 scrape 출력의
         * {@code quantile} 라벨로만 내보낸다 — 지금은 후자다.
         *
         * <p>Timer 자체의 measurement 에 백분위가 없는 것은 양쪽 공통이고(COUNT · TOTAL_TIME ·
         * MAX 뿐), 스냅샷에는 남아 있어 {@code percentileNanos} 는 레지스트리와 무관하다.
         */
        @Test
        @DisplayName("요청이 지나가면 0.99 가 실린다 — Timer 스냅샷과 .percentile 게이지 양쪽에")
        void the99thPercentileIsRecordedAndExposed() throws Exception {
            HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(
                            URI.create("http://localhost:" + port + "/obs3-probe")).build(),
                    HttpResponse.BodyHandlers.ofString());

            MeterValueReader reader = new MeterValueReader(registry);
            // 기록은 응답이 나간 뒤에 끝난다. 폴링하지 않으면 간헐적으로 비어 있다.
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(reader.percentileNanos(MeterNames.HTTP_SERVER_REQUESTS, 0.99))
                            .as("observation.yml 의 percentiles 가 자동 계측 Timer 에 걸려야 한다")
                            .isPresent());

            String scrape = ((PrometheusMeterRegistry) registry).scrape();
            assertThat(scrape)
                    .as("Prometheus 가 긁어갈 형태 — 이 줄이 없으면 화면에서 0.99 를 볼 수 없다")
                    .contains("http_server_requests_seconds{")
                    .contains("quantile=\"0.99\"");
            assertThat(scrape)
                    .as("publishPercentiles 는 버킷을 만들지 않는다 — histogram_quantile 로 "
                            + "병합할 수 없고, 그래서 인스턴스 최댓값을 쓴다")
                    .doesNotContain("http_server_requests_seconds_bucket");
        }
    }

    /*
     * '꺼짐' 쪽 가드 테스트는 두지 않았다. Registry 가 JVM 전역이라 컨텍스트별 프로퍼티
     * override 로 격리되지 않고, 먼저 뜬 컨텍스트가 켜 놓으면 끈 쪽에서도 미터가 보인다.
     * 순서에 따라 결과가 달라지는 테스트는 가드가 아니라 소음이다.
     */

    /**
     * 자동설정을 하나도 끄지 않는다. storage 가 테스트 클래스패스에 있어 JPA 저장소 빈이
     * 올라오고 그것들이 {@code entityManagerFactory} 를 요구한다 — 끄면 배선이 무너져서
     * 미터와 무관한 이유로 깨진다.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @RestController
    static class TestApp {

        @GetMapping("/obs3-probe")
        String probe() {
            return "ok";
        }
    }
}
