package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * P-1(CY-213) 이 세운 scrape 설정이 <b>스스로 모순되지 않는지</b>를 본다.
 *
 * <p>이 계약은 네 파일에 걸쳐 있다.
 * <ul>
 *   <li>{@code infra/prometheus/prometheus.yml} — 어느 호스트:포트를 긁을지
 *   <li>{@code docker-compose.yml} — 그 호스트 이름을 만드는 곳이자 설정 파일을 마운트하는 곳
 *   <li>{@code api/src/main/resources/management.yml.example} — api 관리 포트 기본값
 *   <li>{@code batch/src/main/resources/management.yml.example} — batch 관리 포트 기본값
 * </ul>
 *
 * <p><b>파일마다 따로 검증하는 테스트로는 이걸 못 잡는다.</b> 각 파일은 혼자서는 항상
 * 유효하다 — 포트를 한쪽만 바꿔도 양쪽 다 문법이 맞고 앱은 정상 기동한다. 깨지는 것은
 * scrape 뿐이고, 그건 조용히 {@code up=0} 으로만 나타난다. 그래서 한 테스트가 네 파일을
 * 함께 읽는다.
 *
 * <p><b>잡지 못하는 것 ① — 대상이 실제로 응답하는지.</b> 그건 HTTP 가 필요하고
 * {@code BatchManagementExposureTest}(batch 모듈)와
 * {@code PrometheusExposureContractTest}(api)가 각자 자기 모듈에서 본다. 이 클래스는
 * "긁으러 갈 주소가 맞는가" 까지다.
 *
 * <p><b>잡지 못하는 것 ② — 런타임 env override.</b> 여기서 대조하는 것은 {@code .example} 의
 * <b>기본값</b>뿐이다. batch 를 {@code BATCH_MANAGEMENT_PORT=9095} 로 띄우면 이 테스트는
 * 초록불인데 {@code up=0} 이다. prometheus.yml 은 env 치환을 하지 않으므로(Prometheus 가
 * 설정 파일에서 환경변수를 풀어 주지 않는다) 이 중복은 테스트로 없앨 수 없다 — 포트를
 * env 로 옮기려면 compose 가 양쪽에 같은 값을 주입하는 구조가 되어야 하고, 그건 앱
 * 서비스가 compose 에 붙은 뒤의 일이다.
 *
 * <p>TODO(CY-213 후속, @SH-Seol): compose 에 api·batch 서비스가 붙으면 관리 포트를 compose
 * 의 한 곳에서 정의해 양쪽에 주입하고, 이 항목을 지운다.
 */
class PrometheusScrapeConfigContractTest {

    /** {@code ${VAR:기본값}} 에서 기본값만 꺼낸다 — 컨테이너는 env 주입 없이 뜬다. */
    private static String defaultOf(String placeholder) {
        int colon = placeholder.lastIndexOf(':');
        return placeholder.substring(colon + 1, placeholder.length() - 1);
    }

    @Test
    @DisplayName("scrape 대상은 api · batch 둘이고 관리 포트를 가리킨다 — 앱 포트로 긁으면 404 다")
    void bothTargetsPointAtTheManagementPortsDeclaredByEachModule() throws IOException {
        Path repo = repoRoot();
        Map<String, Object> prometheus = loadYaml(repo.resolve("infra/prometheus/prometheus.yml"));

        Map<String, String> targets = targetsByJob(prometheus);
        assertThat(targets.keySet())
                .as("대상이 둘이어야 한다. batch 를 빠뜨리면 정합성 gap · 대기열 길이 ·"
                        + " Kafka lag 이 통째로 안 들어온다")
                .containsExactlyInAnyOrder("api", "batch");

        assertThat(targets.get("api"))
                .as("api 관리 포트는 management.yml.example 이 정한다. 여기만 고치면 scrape 가"
                        + " 조용히 404 가 된다")
                .isEqualTo("api:" + managementPortDefault(
                        repo.resolve("api/src/main/resources/management.yml.example")));

        assertThat(targets.get("batch"))
                .as("batch 관리 포트도 마찬가지다")
                .isEqualTo("batch:" + managementPortDefault(
                        repo.resolve("batch/src/main/resources/management.yml.example")));
    }

    @Test
    @DisplayName("두 관리 포트는 서로 다르다 — 로컬에서 비컨테이너로 같이 띄우면 부딪힌다")
    void theTwoManagementPortsDoNotCollide() throws IOException {
        Path repo = repoRoot();
        String api = managementPortDefault(
                repo.resolve("api/src/main/resources/management.yml.example"));
        String batch = managementPortDefault(
                repo.resolve("batch/src/main/resources/management.yml.example"));

        assertThat(api)
                .as("compose 네트워크 안에서는 호스트가 달라 같아도 돌아간다. 그래서 이 충돌은"
                        + " 컨테이너로 돌리는 한 안 드러나고, 노트북에서 둘을 같이 띄우는"
                        + " 순간에만 터진다 — 그때 원인을 찾기 어렵다")
                .isNotEqualTo(batch);
    }

    @Test
    @DisplayName("긁는 두 모듈 다 allowlist 에 prometheus 를 열어 둔다 — 닫히면 조용히 404 다")
    void everyScrapedModuleOpensThePrometheusEndpoint() throws IOException {
        Path repo = repoRoot();

        // 둘 다 우리 티켓 밖에서 열렸다 — api 는 CY-246, batch 는 CY-247. 여기서 다시 보는
        // 이유는 소유가 아니라 의존이다: 저쪽이 닫히는 순간 이 파일의 대상 둘이 통째로
        // 무의미해지는데, 그 사실은 up=0 말고는 아무 데도 안 나타난다.
        for (String module : List.of("api", "batch")) {
            assertThat(exposureInclude(
                    repo.resolve(module + "/src/main/resources/management.yml.example")))
                    .as("%s 의 allowlist 에서 prometheus 가 빠지면 job=\"%s\" 가 영영 up=0 이다."
                            + " 이 파일이 그 모듈을 긁고 있는 한 함께 봐야 한다", module, module)
                    .contains("prometheus");
        }
    }

    @Test
    @DisplayName("batch 는 관측 풀과 health 상태 계약을 함께 켠다 — 한쪽만 켜면 기동에서 죽는다")
    void batchDoesNotEnableTheObservationPoolWithoutTheHealthContract() throws IOException {
        Path repo = repoRoot();
        Path application = repo.resolve("batch/src/main/resources/application.yml.example");
        Path management = repo.resolve("batch/src/main/resources/management.yml.example");

        if (!observationPoolEnabled(application)) {
            // 지금 상태다. api 의 order/group 블록을 batch 에 미리 복사해 두면 안 된다 —
            // 실측: group.obs 만 넣고 띄우면 Boot 가 "Health contributor 'obsDb' defined in
            // 'management.endpoint.health.group.obs.include' does not exist" 로 기동을 멈춘다.
            assertThat(healthNodeOf(management))
                    .as("관측 풀이 꺼져 있는데 group 을 미리 적으면 batch 가 아예 못 뜬다")
                    .doesNotContainKey("group");
            return;
        }

        // 켜는 순간 두 파일이 한 쌍이 된다. 켜기만 하고 여기를 안 채우면 관측 풀 장애가 합산
        // 상태를 끌어내려 /actuator/health 가 503 이 되고, 배치가 죽은 것처럼 보인다.
        @SuppressWarnings("unchecked")
        Map<String, Object> status = (Map<String, Object>) healthNodeOf(management).get("status");
        assertThat(status)
                .as("observation.datasource.enabled 를 켰으면 status.order 가 있어야 한다."
                        + " 목록에 없는 상태는 '가장 안 심각' 으로 취급된다")
                .isNotNull();

        List<String> order = statusOrder(status.get("order"));
        assertThat(order.indexOf("OBSERVATION_DOWN"))
                .as("OBSERVATION_DOWN 이 UP 보다 뒤여야 관측 풀 장애만으로 인스턴스가"
                        + " 로드밸런서에서 빠지지 않는다")
                .isGreaterThan(order.indexOf("UP"));
        assertThat(order.indexOf("DOWN"))
                .as("반대로 DOWN 이 UP 보다 앞이어야 진짜 장애가 200 에 묻히지 않는다")
                .isBetween(0, order.indexOf("UP") - 1);

        assertThat(healthNodeOf(management))
                .as("group.obs 가 없으면 관측 풀 장애를 볼 창구가 사라진다 — 합산은 UP 이라"
                        + " 어디에도 안 드러난다")
                .containsKey("group");
    }

    @Test
    @DisplayName("scrape_timeout 은 interval 보다 작다 — 크면 Prometheus 가 기동조차 못 한다")
    void theTimeoutFitsInsideTheInterval() throws IOException {
        Map<String, Object> global = globalOf(loadYaml(
                repoRoot().resolve("infra/prometheus/prometheus.yml")));

        assertThat(millis(String.valueOf(global.get("scrape_timeout"))))
                .as("겹쳐 돌면 샘플 타임스탬프가 밀려 count_over_time(up[1m]) 이 60 미만이 된다."
                        + " 실측 — timeout 을 2s 로 두면 promtool 이 \"global scrape timeout"
                        + " greater than scrape interval\" 로 거부한다")
                .isLessThan(millis(String.valueOf(global.get("scrape_interval"))));

        assertThat(millis(String.valueOf(global.get("scrape_interval"))))
                .as("1초 간격이 이 티켓의 전제다. 바꾸면 count_over_time(up[1m])==60 검증이"
                        + " 함께 바뀌어야 한다")
                .isEqualTo(1000L);
    }

    @Test
    @DisplayName("Prometheus 를 호스트로 노출하지 않는다 — 인증이 없어 모든 지표가 열린다")
    void thePrometheusServicePublishesNoHostPort() throws IOException {
        Map<String, Object> service = prometheusService();

        assertThat(service.get("ports"))
                .as("ports 를 여는 순간 인증 없는 Prometheus 가 호스트에 열린다. 관리 포트를"
                        + " 잠근 것(OBS-20)과 같은 이유다. 화면은 우리 API 가 compose 네트워크"
                        + " 안에서 대신 읽는다")
                .isNull();

        assertThat(String.join(" ", commandOf(service)))
                .as("--web.enable-lifecycle 은 인증 없는 /-/reload · /-/quit 를 연다."
                        + " 포트를 안 열었어도 같은 네트워크의 컨테이너는 부를 수 있다")
                .doesNotContain("--web.enable-lifecycle");
    }

    @Test
    @DisplayName("보관 기간이 프로젝트 기간을 덮는다 — 플래그를 지우면 기본 15일로 떨어진다")
    void retentionCoversTheWholeProject() throws IOException {
        String retention = flagValue(commandOf(prometheusService()), "--storage.tsdb.retention.time");

        // 이 플래그가 없으면 Prometheus 는 기본 15d 로 돈다. 그 사실은 어디에도 안 적히고,
        // 3주째 회차를 다시 보려 할 때 "데이터가 없다" 로만 나타난다. 실측 — 이 단언을 넣기
        // 전에는 플래그를 통째로 지워도 모든 테스트가 통과했다.
        assertThat(days(retention))
                .as("프로젝트를 3주 연속 켜도 실측 디스크 증가가 1.3GB 미만이라(시간당 2.5MB)"
                        + " 회차를 골라 지울 이유가 없다. 이 값을 줄이려면 OBS-22 의 사본이"
                        + " 먼저 있어야 한다")
                .isGreaterThanOrEqualTo(21);
    }

    @Test
    @DisplayName("TSDB 는 named volume 위에 있다 — 컨테이너를 지워도 과거 회차가 남아야 한다")
    void theTsdbLivesOnANamedVolumeThatTheCommandActuallyWritesTo() throws IOException {
        Map<String, Object> compose = loadYaml(repoRoot().resolve("docker-compose.yml"));
        Map<String, Object> service = prometheusService();

        String tsdbPath = flagValue(commandOf(service), "--storage.tsdb.path");
        List<String> volumes = stringList(service.get("volumes"));

        String mountedName = volumes.stream()
                .filter(v -> v.split(":")[1].equals(tsdbPath))
                .map(v -> v.split(":")[0])
                .findFirst()
                .orElse(null);

        assertThat(mountedName)
                .as("--storage.tsdb.path(%s) 에 아무것도 안 붙어 있으면 컨테이너를 지우는 순간"
                        + " 부하 회차 데이터가 같이 사라진다. 실측 — named volume 을 붙이면"
                        + " 컨테이너를 rm 하고 다시 만들어도 이전 샘플이 질의된다", tsdbPath)
                .isNotNull();

        @SuppressWarnings("unchecked")
        Map<String, Object> declared = (Map<String, Object>) compose.get("volumes");
        assertThat(declared)
                .as("bind mount(./ 로 시작)가 아니라 최상위에 선언된 named volume 이어야 한다."
                        + " bind 로 바꾸면 호스트 uid 와 nobody(65534)가 어긋나 기동에 실패한다")
                .containsKey(mountedName);

        String configFile = flagValue(commandOf(service), "--config.file");
        assertThat(volumes)
                .as("--config.file 이 가리키는 경로에 우리 prometheus.yml 이 실제로 마운트돼"
                        + " 있어야 한다. 한쪽만 고치면 컨테이너가 기본 설정으로 뜬다")
                .anyMatch(v -> v.startsWith("./infra/prometheus/prometheus.yml:" + configFile));
    }

    @Test
    @DisplayName("회차 식별자를 label 로 넣지 않는다 — 회차마다 시계열 집합이 통째로 갈린다")
    void noRunIdentifierLeaksIntoLabels() throws IOException {
        Path config = repoRoot().resolve("infra/prometheus/prometheus.yml");

        assertThat(globalOf(loadYaml(config)))
                .as("external_labels 는 전 시계열에 라벨을 하나씩 붙인다")
                .doesNotContainKey("external_labels");

        assertThat(statementLines(Files.readString(config)))
                .as("회차 경계는 라벨이 아니라 benchmark_runs 의 started_at ~ stopped_at 으로"
                        + " 질의 범위를 잘라 표현한다")
                .noneMatch(line -> line.contains("benchmark_run_id"));
    }

    // ── 읽기 도우미 ───────────────────────────────────────────────────────────

    /** 주석은 이 검사들의 대상이 아니다 — 금지 사항을 설명하는 주석이 전부 오탐이 된다. */
    private List<String> statementLines(String yaml) {
        return yaml.lines().map(String::strip).filter(l -> !l.startsWith("#")).toList();
    }

    private Map<String, Object> loadYaml(Path file) throws IOException {
        assertThat(file).as("계약에 걸린 파일이 없다").exists();
        try (var in = Files.newInputStream(file)) {
            return new Yaml().load(in);
        }
    }

    private Map<String, Object> globalOf(Map<String, Object> prometheus) {
        @SuppressWarnings("unchecked")
        Map<String, Object> global = (Map<String, Object>) prometheus.get("global");
        return global;
    }

    private Map<String, String> targetsByJob(Map<String, Object> prometheus) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobs =
                (List<Map<String, Object>>) prometheus.get("scrape_configs");

        Map<String, String> byJob = new java.util.LinkedHashMap<>();
        for (Map<String, Object> job : jobs) {
            assertThat(job.get("metrics_path"))
                    .as("%s — actuator 기본 경로가 아니면 404 다", job.get("job_name"))
                    .isEqualTo("/actuator/prometheus");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> statics =
                    (List<Map<String, Object>>) job.get("static_configs");
            byJob.put(String.valueOf(job.get("job_name")),
                    stringList(statics.get(0).get("targets")).get(0));
        }
        return byJob;
    }

    private Map<String, Object> prometheusService() throws IOException {
        Map<String, Object> compose = loadYaml(repoRoot().resolve("docker-compose.yml"));
        @SuppressWarnings("unchecked")
        Map<String, Object> services = (Map<String, Object>) compose.get("services");
        @SuppressWarnings("unchecked")
        Map<String, Object> prometheus = (Map<String, Object>) services.get("prometheus");
        assertThat(prometheus).as("compose 에 prometheus 서비스가 없다").isNotNull();
        return prometheus;
    }

    private List<String> commandOf(Map<String, Object> service) {
        return stringList(service.get("command"));
    }

    private List<String> stringList(Object value) {
        @SuppressWarnings("unchecked")
        List<Object> raw = (List<Object>) value;
        return raw.stream().map(String::valueOf).toList();
    }

    private String flagValue(List<String> command, String flag) {
        return command.stream()
                .filter(arg -> arg.startsWith(flag + "="))
                .map(arg -> arg.substring(flag.length() + 1))
                .findFirst()
                .orElseThrow(() -> new AssertionError(flag + " 가 command 에 없다"));
    }

    private String managementPortDefault(Path managementYml) throws IOException {
        Map<String, Object> root = loadYaml(managementYml);
        @SuppressWarnings("unchecked")
        Map<String, Object> management = (Map<String, Object>) root.get("management");
        @SuppressWarnings("unchecked")
        Map<String, Object> server = (Map<String, Object>) management.get("server");
        return defaultOf(String.valueOf(server.get("port")));
    }

    /**
     * {@code status.order} 는 YAML 리스트로도, 쉼표 문자열로도 쓸 수 있다 — Spring 의 Binder 가
     * 양쪽을 다 받는다. api 의 example 은 쉼표 문자열이다.
     *
     * <p>이걸 리스트로만 읽었다가 실제로 깨졌다: 올바른 설정을 넣어 봤더니 통과가 아니라
     * ClassCastException 이 났다. 가드를 일부러 깨뜨려 보지 않았으면 못 봤다 — 잘못된 설정에서는
     * 어차피 실패라 통과/실패만 보면 똑같아 보였다.
     */
    private List<String> statusOrder(Object value) {
        if (value instanceof String commaSeparated) {
            return List.of(commaSeparated.split("\\s*,\\s*"));
        }
        return stringList(value);
    }

    /** {@code 30d} 같은 보관 기간을 일 수로 바꾼다. 다른 단위는 읽지 않는다 — 여기서 멈춘다. */
    private long days(String retention) {
        if (retention.endsWith("d")) {
            return Long.parseLong(retention.substring(0, retention.length() - 1));
        }
        throw new AssertionError("모르는 보관 기간 단위: " + retention
                + " — 일(d) 단위로 쓰거나 여기를 넓혀라");
    }

    /** {@code management.endpoint.health} 하위 노드. 없으면 빈 맵이다. */
    private Map<String, Object> healthNodeOf(Path managementYml) throws IOException {
        Map<String, Object> node = loadYaml(managementYml);
        for (String key : List.of("management", "endpoint", "health")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> next = (Map<String, Object>) node.get(key);
            if (next == null) {
                return Map.of();
            }
            node = next;
        }
        return node;
    }

    /**
     * {@code observation.datasource.enabled} 는 여러 문서(---)에 흩어져 있을 수 있어 마지막
     * 문서까지 훑는다. 첫 문서만 보면 나중에 켜도 못 알아챈다.
     */
    private boolean observationPoolEnabled(Path applicationYml) throws IOException {
        try (var in = Files.newInputStream(applicationYml)) {
            for (Object document : new Yaml().loadAll(in)) {
                if (!(document instanceof Map<?, ?> map)) {
                    continue;
                }
                Object observation = map.get("observation");
                if (observation instanceof Map<?, ?> node
                        && node.get("datasource") instanceof Map<?, ?> datasource
                        && Boolean.TRUE.equals(datasource.get("enabled"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private String exposureInclude(Path managementYml) throws IOException {
        Map<String, Object> node = loadYaml(managementYml);
        for (String key : List.of("management", "endpoints", "web", "exposure")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> next = (Map<String, Object>) node.get(key);
            node = next;
        }
        return String.valueOf(node.get("include"));
    }

    /**
     * Prometheus 의 기간 표기를 ms 로 바꾼다. snakeyaml 은 {@code 900ms} 를 문자열로 주므로
     * 단위를 우리가 읽어야 한다 — 숫자만 비교하면 {@code 900ms} 가 {@code 1s} 보다 커진다.
     */
    private long millis(String duration) {
        if (duration.endsWith("ms")) {
            return Long.parseLong(duration.substring(0, duration.length() - 2));
        }
        if (duration.endsWith("s")) {
            return Long.parseLong(duration.substring(0, duration.length() - 1)) * 1000L;
        }
        throw new AssertionError("모르는 기간 단위: " + duration
                + " — 이 테스트가 읽을 수 있는 단위(ms · s)로 쓰거나 여기를 넓혀라");
    }

    /**
     * 설정 파일은 클래스패스가 아니라 저장소 루트에 있다. Gradle(모듈 디렉터리)과
     * IDE(저장소 루트) 양쪽에서 실행되므로 {@code settings.gradle} 을 표지로 거슬러 올라간다 —
     * 못 찾으면 skip 이 아니라 실패한다. skip 하면 아무것도 검사하지 않으면서 초록불이 된다.
     */
    private Path repoRoot() {
        Path candidate = Path.of("").toAbsolutePath();
        while (candidate != null) {
            if (Files.isRegularFile(candidate.resolve("settings.gradle"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException(
                "저장소 루트를 찾지 못했다. 실행 디렉터리: " + Path.of("").toAbsolutePath());
    }
}
