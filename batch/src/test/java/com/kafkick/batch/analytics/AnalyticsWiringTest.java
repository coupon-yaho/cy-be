package com.kafkick.batch.analytics;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.support.CronExpression;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * 집계 배선이 <b>실제 기동에서</b> 서는지, 그리고 시간대 계약이 두 파일에서 같은 값인지 본다.
 *
 * <p>배선과 계약을 한 파일에 둔 이유 — 둘은 같은 실패의 앞뒤다. 배선이 서 있어도 시간대가
 * 어긋나면 화면의 날짜와 저장된 날짜가 조용히 밀리고, 시간대가 맞아도 배선이 없으면 집계가
 * 아예 안 돈다. 어느 쪽도 기동 로그에는 안 남는다.
 */
class AnalyticsWiringTest {

    /** api 가 분석 조회에 쓰는 유일한 시간대. batch 는 이 값으로 <b>미리</b> 버킷팅해 저장한다. */
    private static final Pattern API_ZONE = Pattern.compile(
            "ANALYTICS_ZONE\\s*=\\s*ZoneId\\.of\\(\"([^\"]+)\"\\)");

    private static final Pattern BATCH_CRON = Pattern.compile(
            "(?m)^\\s*analytics-cron:\\s*\"([^\"]+)\"");

    @Nested
    @DisplayName("시간대 계약 — api 상수와 batch 설정")
    class ZoneContract {

        /**
         * 두 값을 각각 검증하는 테스트 두 개로는 이 계약을 못 지킨다 — 한쪽만 바꿔도 둘 다 통과한다.
         * 그래서 <b>같은 테스트가 두 파일을 읽어</b> 맞춰 본다.
         */
        @Test
        @DisplayName("batch 의 집계 시간대가 api 의 ANALYTICS_ZONE 과 같다")
        void batchZoneMatchesApiConstant() {
            String apiZone = extract(API_ZONE, read(
                    "api/src/main/java/com/kafkick/api/admin/dashboard/AdminDashboardController.java"));
            // 정규식으로 첫 zone: 을 집으면 키 경로를 확인하지 않는다 — 다른 블록의 zone 이
            // 위로 올라오는 순간 조용히 엉뚱한 값을 비교한다. YAML 로 읽어 경로를 못 박는다.
            String batchZone = yamlValue(
                    "batch/src/main/resources/application.yml.example", "batch", "analytics", "zone");

            assertThat(batchZone)
                    .as("어긋나면 화면의 발급일과 저장된 발급일이 조용히 밀린다")
                    .isEqualTo(apiZone);
            assertThat(ZoneId.of(batchZone)).isEqualTo(ZoneId.of(apiZone));
            // ⚠️ env 로 열려 있으면 이 비교가 헛돈다 — 파일에 적힌 기본값이 같아도 배포에서
            //    ANALYTICS_ZONE 하나로 버킷이 9시간 밀리기 때문이다. 손잡이 자체를 막는다.
            // 위 추출 자체가 리터럴만 받는다. 손잡이가 되살아나면 추출이 실패해 이 테스트가 죽는다.
            assertThat(read("batch/src/main/resources/application.yml.example"))
                    .as("집계 시간대를 env 로 덮을 수 있으면 이 계약은 파일 비교로 못 지킨다")
                    .doesNotContain("${ANALYTICS_ZONE");

            // ⚠️ 루트 application.yml 은 compose 가 컨테이너에 마운트하는 **오버레이**다.
            //    거기에 batch.analytics.zone 을 적으면 모듈 기본값을 덮어써서, 위 비교는 초록인
            //    채로 배포에서만 버킷이 밀린다. 그래서 그 키가 아예 없어야 한다.
            assertThat(yamlValueOrNull("application.yml.example", "batch", "analytics", "zone"))
                    .as("루트 오버레이가 집계 시간대를 덮으면 모듈 파일만 봐서는 이 계약을 못 지킨다")
                    .isNull();
        }

        /**
         * 크론과 {@code @Scheduled} 자리 표시자는 이름이 어긋나면 <b>기동에서</b> 죽는다(아래 배선
         * 테스트가 본다). 여기서는 그 값이 실제로 1시간 주기인지를 본다 — A 와 확정한 주기이고,
         * {@code admin.analytics.stale-after}(3시간)가 그 주기를 전제로 잡힌 값이다.
         */
        @Test
        @DisplayName("집계 크론이 1시간 주기다")
        void cronRunsHourly() {
            CronExpression cron = CronExpression.parse(
                    extract(BATCH_CRON, read("batch/src/main/resources/application.yml.example")));
            java.time.LocalDateTime first = cron.next(
                    java.time.LocalDateTime.parse("2026-08-26T00:00:00"));
            java.time.LocalDateTime second = cron.next(first);

            assertThat(java.time.Duration.between(first, second))
                    .isEqualTo(java.time.Duration.ofHours(1));
        }
    }

    @Nested
    @DisplayName("배선")
    @SpringBootTest(properties = "spring.flyway.enabled=true")
    @Import(MySqlContainerConfig.class)
    class Wiring {

        @Autowired
        private ApplicationContext context;

        /**
         * {@code @Scheduled} 의 {@code ${batch.schedule.analytics-cron}} 은 값이 없으면 기동에서
         * 죽는다. 즉 이 테스트가 서는 것 자체가 크론 키와 코드가 이어져 있다는 증거다.
         */
        @Test
        @DisplayName("집계 빈 셋이 기동에 선다")
        void beansAreWired() {
            assertThat(context.getBean(AnalyticsAggregateReader.class)).isNotNull();
            assertThat(context.getBean(AnalyticsRunStore.class)).isNotNull();
            assertThat(context.getBean(AnalyticsAggregationRunner.class)).isNotNull();
            assertThat(context.getBean(AnalyticsAggregationScheduler.class)).isNotNull();
            assertThat(context.getBean(AnalyticsAggregationController.class)).isNotNull();
        }
    }

    @Nested
    @DisplayName("관측을 끄면 집계도 함께 사라진다")
    @SpringBootTest(properties = {
            "observation.datasource.enabled=false",
            "observation.domain-gauge.enabled=false",
            "spring.flyway.enabled=true"
    })
    @Import(MySqlContainerConfig.class)
    class WithoutObservation {

        @Autowired
        private ApplicationContext context;

        /**
         * 읽기가 관측 풀로 나가므로 그 풀이 없으면 배선할 대상이 없다. <b>기동은 살린다</b> —
         * 관측 스위치가 배치 프로세스를 멈춰 세우는 것은 풀을 나눈 취지와 반대다.
         *
         * <p>⚠️ 대가 — 관측을 끄면 집계가 조용히 멈추고, 화면에서는 3시간 뒤 STALE 로 나타난다.
         * 이 조합을 배포 시점에 잡는 그물은 없다. 여기 적어 두는 것이 전부다.
         */
        @Test
        @DisplayName("집계 빈이 없고 기동은 산다")
        void aggregationIsAbsentButStartupSurvives() {
            assertThat(context.getBeanNamesForType(AnalyticsAggregationRunner.class)).isEmpty();
            assertThat(context.getBeanNamesForType(AnalyticsAggregationScheduler.class)).isEmpty();
            // ⚠️ 컨트롤러는 @RestController 라 컴포넌트 스캔에 걸린다. 같은 조건을 안 지면
            //    Runner 를 못 찾아 **기동이 죽는다** — 조용히 사라지는 것이 아니라 배포가 실패한다.
            assertThat(context.getBeanNamesForType(AnalyticsAggregationController.class)).isEmpty();
        }
    }

    /**
     * 키 경로가 있으면 값을, 없으면 {@code null} 을 준다. "그 키가 없어야 한다" 는 계약용이다.
     *
     * <p>⚠️ <b>문서 전체</b>를 훑는다. 루트 설정은 프로파일마다 {@code ---} 로 갈린 다중 문서라,
     * 첫 문서만 읽으면 뒤 프로파일에 숨은 키를 못 본다(실측 — 한 문서만 읽으면 파서가 아예 죽는다).
     */
    private static String yamlValueOrNull(String relativePath, String... path) {
        for (Object document : new org.yaml.snakeyaml.Yaml().loadAll(read(relativePath))) {
            String found = walk(document, path);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static String walk(Object root, String... path) {
        Object node = root;
        for (String key : path) {
            if (!(node instanceof java.util.Map<?, ?> map) || !map.containsKey(key)) {
                return null;
            }
            node = ((java.util.Map<String, Object>) map).get(key);
        }
        return node == null ? null : String.valueOf(node).trim();
    }

    /** 키 경로를 지정해 읽는다. 값이 없거나 중간 노드가 맵이 아니면 그 자리에서 실패한다. */
    @SuppressWarnings("unchecked")
    private static String yamlValue(String relativePath, String... path) {
        Object node = new org.yaml.snakeyaml.Yaml().load(read(relativePath));
        StringBuilder walked = new StringBuilder();
        for (String key : path) {
            walked.append(walked.isEmpty() ? "" : ".").append(key);
            if (!(node instanceof java.util.Map<?, ?> map) || !map.containsKey(key)) {
                throw new IllegalStateException("설정 키가 없다: " + walked);
            }
            node = ((java.util.Map<String, Object>) map).get(key);
        }
        return String.valueOf(node).trim();
    }

    private static String extract(Pattern pattern, String content) {
        Matcher matcher = pattern.matcher(content);
        if (!matcher.find()) {
            throw new IllegalStateException("계약 값을 찾지 못했다: " + pattern);
        }
        return matcher.group(1).trim();
    }

    private static String read(String relativePath) {
        Path path = repositoryRoot().resolve(relativePath);
        try {
            return Files.readString(path);
        } catch (IOException exception) {
            throw new UncheckedIOException("계약 파일을 읽을 수 없다: " + path, exception);
        }
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("settings.gradle"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("저장소 루트를 찾을 수 없다.");
    }
}
