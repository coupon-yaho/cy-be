package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static com.kafkick.api.observation.ConfigContractFixture.defaultOf;
import static com.kafkick.api.observation.ConfigContractFixture.loadYaml;
import static com.kafkick.api.observation.ConfigContractFixture.repoRoot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P-1(CY-213) 이 세운 scrape 설정이 <b>스스로 모순되지 않는지</b>를 본다.
 *
 * <p>이 계약은 여섯 파일에 걸쳐 있다.
 * <ul>
 *   <li>{@code infra/prometheus/prometheus.yml} — 어느 호스트:포트를 긁을지
 *   <li>{@code infra/prometheus/targets/queue-gateway.yml[.template]} — 외부 타겟의 비활성 기본값과 배포 템플릿
 *   <li>{@code compose.yml} — 그 호스트 이름을 만드는 곳이자 설정 파일을 마운트하는 곳
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
 * <p>compose 에는 이미 api·batch 서비스가 있으므로 구조상으로는 지금 할 수 있다 — 관리 포트를
 * compose 가 한 곳에서 정의해 앱 env 와 prometheus.yml 양쪽에 주입하는 형태다. 다만
 * prometheus.yml 쪽은 여전히 env 치환이 안 되므로 대상 주소를 compose 가 만들어 주입하는
 * 설계가 필요하고, 그 설계 변경 전까지 이 테스트는 모듈 기본값과 설정 파일의 일치를 지킨다.
 */
class PrometheusScrapeConfigContractTest {

    /**
     * Docker Compose 가 실제로 읽는 이름. {@code docker-compose.yml} 을 함께 두면 이쪽만
     * 읽히고 저쪽은 조용히 무시된다 — 그래서 이름을 하나로 고정하고 아래에서 부재를 본다.
     */
    private static final String COMPOSE_FILE = "compose.yml";

    private static final List<String> OTHER_COMPOSE_FILES =
            List.of("compose.yaml", "docker-compose.yaml", "docker-compose.yml");

    @Test
    @DisplayName("scrape 대상은 api · batch · queue-gateway이고 각 수집 계약을 가리킨다")
    void allTargetsPointAtTheDeclaredScrapeContracts() throws IOException {
        Path repo = repoRoot();
        Map<String, Object> prometheus = loadYaml(repo.resolve("infra/prometheus/prometheus.yml"));

        Map<String, String> targets = targetsByJob(prometheus);
        assertThat(targets.keySet())
                .as("queue-gateway가 빠지면 대기 인원과 게이트 판정 실패를 운영 관제가 알 수 없다")
                .containsExactlyInAnyOrder("api", "batch", "queue-gateway");

        assertThat(targets.get("api"))
                .as("api 관리 포트는 management.yml.example 이 정한다. 여기만 고치면 scrape 가"
                        + " 조용히 404 가 된다")
                .isEqualTo("api:" + managementPortDefault(
                        repo.resolve("api/src/main/resources/management.yml.example")));

        assertThat(targets.get("batch"))
                .as("batch 관리 포트도 마찬가지다")
                .isEqualTo("batch:" + managementPortDefault(
                        repo.resolve("batch/src/main/resources/management.yml.example")));

        assertThat(targets.get("queue-gateway"))
                .as("외부 서버 주소는 배포 환경이 file_sd 파일로 공급한다")
                .isEqualTo("targets/queue-gateway.yml");

        assertThat(Files.readString(repo.resolve("infra/prometheus/targets/queue-gateway.yml")).strip())
                .as("기본 Compose에는 외부 게이트웨이가 없으므로 타겟과 알림이 비활성이어야 한다")
                .isEqualTo("[]");

        assertThat(Files.readString(repo.resolve(
                "infra/prometheus/targets/queue-gateway.yml.template")))
                .as("배포자가 외부 주소와 문서화된 8081 포트를 주입할 템플릿이 필요하다")
                .contains(":8081");
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
    @DisplayName("성능 회차 설정은 API 발견 방식만 바꾸고 외부 게이트웨이·알림은 보존한다")
    void performanceScrapePreservesGatewayAndAlertWiring() throws IOException {
        Map<String, Object> baseline = loadYaml(
                repoRoot().resolve("infra/prometheus/prometheus.yml"));
        Map<String, Object> performance = loadYaml(
                repoRoot().resolve("perf/env/prometheus.perf.yml"));

        assertThat(performance.get("alerting")).isEqualTo(baseline.get("alerting"));
        assertThat(performance.get("rule_files")).isEqualTo(baseline.get("rule_files"));
        assertThat(jobNamed(performance, "queue-gateway"))
                .as("회차 오버레이가 기본 설정을 덮어쓰므로 게이트웨이 job도 다시 적어야 한다")
                .isEqualTo(jobNamed(baseline, "queue-gateway"));
    }

    @Test
    @DisplayName("기본 Compose에서 Prometheus 알림이 Alertmanager와 sink까지 전달된다")
    void composeProvidesTheWholeAlertDeliveryPath() throws IOException {
        Map<String, Object> prometheus = serviceNamed("prometheus");
        Map<String, Object> alertmanager = serviceNamed("alertmanager");
        Map<String, Object> sink = serviceNamed("alert-sink");

        assertThat(stringList(prometheus.get("depends_on"))).contains("alertmanager");
        assertThat(stringList(alertmanager.get("depends_on"))).contains("alert-sink");
        assertThat(stringList(alertmanager.get("volumes")))
                .anyMatch(volume -> volume.startsWith(
                        "./infra/alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml"));
        assertThat(stringList(sink.get("volumes")))
                .anyMatch(volume -> volume.startsWith(
                        "./infra/alertmanager/alert-sink.py:/app/alert-sink.py"));
        assertThat(alertmanager.get("ports")).as("무인증 Alertmanager를 호스트에 열지 않는다").isNull();
        assertThat(sink.get("ports")).as("목 수신기도 호스트에 열지 않는다").isNull();
    }

    // batch 의 관측 풀 ↔ health group 계약은 여기서 보지 않는다. 두 파일 다 batch 소유이고
    // batch/observation/DomainGaugeConfigContractTest 가 같은 계약을 이미 지킨다. 두 곳에 적어 둔
    // 동안 실제로 갈라졌다 — CY-309 가 스위치를 ${OBSERVATION_DATASOURCE_ENABLED:true} 로 열자
    // 이쪽 판정만 그 표기를 못 읽어, 켜진 설정에 "꺼졌을 때만 유효한 금지 조항" 을 들이대며
    // 빨간불이 됐다. 저쪽은 기본값을 뽑아 읽어서 멀쩡했다.
    //
    // 이 파일의 본업은 "scrape 대상이 실제로 긁히는가" 다. 남의 모듈 설정은 그 모듈이 지킨다.

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
    @DisplayName("compose 파일은 하나다 — 둘을 두면 docker-compose.yml 이 조용히 무시된다")
    void onlyOneComposeFileExists() {
        Path repo = repoRoot();

        assertThat(repo.resolve(COMPOSE_FILE))
                .as("이 테스트가 읽는 파일이 실제로 있어야 한다")
                .exists();

        // 실측 근거 — Docker Compose 는 compose.yaml · compose.yml · docker-compose.yaml ·
        // docker-compose.yml 순으로 찾고 먼저 맞는 하나만 쓴다. 둘을 같이 두면 경고 한 줄만
        // 나오고 뒤쪽 파일의 서비스는 없는 것처럼 동작한다. 에러가 아니라 침묵이라 더 나쁘다.
        assertThat(OTHER_COMPOSE_FILES.stream().filter(name -> Files.exists(repo.resolve(name))))
                .as("다른 기본 compose 이름이 생기면 둘 중 하나가 조용히 죽는다."
                        + " 서비스를 추가할 거면 %s 안에 넣어라", COMPOSE_FILE)
                .isEmpty();
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
    @DisplayName("관리 포트와 batch 업무 포트를 호스트에 열지 않는다 — 인증이 없어 여는 순간 무방비다")
    void noManagementOrBatchPortIsPublishedToTheHost() throws IOException {
        // api 는 8080(업무)만 열고 9090(actuator)은 열지 않는다. actuator 에 인증이 없어서
        // (Spring Security 미도입) 포트를 안 여는 것이 유일한 방어다 — allowlist 의 exclude
        // 한 줄이 지워지면 /actuator/env 가 DB 비밀번호를 그대로 뱉는다(PRD R12).
        assertThat(publishedContainerPorts("api"))
                .as("api 는 업무 포트만 연다. 관리 포트 9090 을 열면 무인증 actuator 가"
                        + " 0.0.0.0 에 붙는다")
                .containsExactly("8080");

        // batch 는 아무것도 열지 않는다. 9091 에 붙는 verify 온디맨드 트리거가 무인증이라,
        // 열어 두면 아무나 300만 행 검증 배치를 돌려 부하 측정 수치를 오염시킬 수 있다.
        assertThat(publishedContainerPorts("batch"))
                .as("batch 는 업무·관리 포트 모두 열지 않는다. 관제 API 가 내부 네트워크에서"
                        + " batch:9091 로 호출하고 Prometheus 가 batch:9092 를 긁는다")
                .isEmpty();
    }

    @Test
    @DisplayName("CODEOWNERS 가 compose 파일을 덮는다 — 이름을 바꾸면 승인 없이 머지된다")
    void codeownersCoversTheComposeFile() throws IOException {
        // 이 파일이 관리 포트 노출 여부를 정하는 유일한 실물 설정이다. 코드오너가 안 걸리면
        // 다음 PR 에서 ports 한 줄이 리뷰 없이 들어온다 — 위 가드가 CI 에서 잡더라도,
        // 애초에 사람이 봐야 하는 변경이다.
        List<String> patterns = Files.readAllLines(repoRoot().resolve(".github/CODEOWNERS"))
                .stream()
                .map(String::strip)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .map(line -> line.split("\\s+")[0])
                .toList();

        // 맨 위의 catch-all(`*`)은 세지 않는다. 그게 이미 전 파일을 덮고 있어서, 그것만
        // 보면 무슨 이름을 쓰든 통과하는 헛단언이 된다 — 실제로 처음엔 그렇게 짰다가
        // 깨뜨려 보고 알았다. 이 파일을 <b>이름으로 지목한</b> 줄이 있어야 한다.
        assertThat(patterns.stream().filter(pattern -> pattern.contains("compose")).toList())
                .as("CODEOWNERS 에 %s 를 이름으로 지목한 항목이 있어야 한다. catch-all 이"
                        + " 덮고 있더라도, 그게 좁혀지는 날 이 파일만 소유자를 잃는다",
                        COMPOSE_FILE)
                .isNotEmpty()
                .anyMatch(this::matchesComposeFile);
    }

    /** {@code /compose*.yml} 같은 루트 고정 글롭이 {@link #COMPOSE_FILE} 을 덮는지 본다. */
    private boolean matchesComposeFile(String pattern) {
        String normalized = pattern.startsWith("/") ? pattern.substring(1) : pattern;
        if (normalized.contains("/")) {
            return false;
        }
        String regex = java.util.Arrays.stream(normalized.split("\\*", -1))
                .map(java.util.regex.Pattern::quote)
                .reduce((left, right) -> left + "[^/]*" + right)
                .orElse("");
        return COMPOSE_FILE.matches(regex);
    }

    @Test
    @DisplayName("compose 가 마운트하는 파일이 저장소에 실재한다 — 없으면 Docker 가 디렉터리를 만든다")
    void everyBindMountSourceExists() throws IOException {
        Path repo = repoRoot();
        @SuppressWarnings("unchecked")
        Map<String, Object> services =
                (Map<String, Object>) loadYaml(repo.resolve(COMPOSE_FILE)).get("services");

        List<String> missing = new java.util.ArrayList<>();
        for (Map.Entry<String, Object> entry : services.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> definition = (Map<String, Object>) entry.getValue();
            if (definition.get("volumes") == null) {
                continue;
            }
            for (String mount : stringList(definition.get("volumes"))) {
                String source = mount.split(":")[0];
                // named volume 은 최상위 volumes 가 만든다. 여기서 보는 것은 bind mount 뿐이다.
                if (!source.startsWith("./") && !source.startsWith("../")) {
                    continue;
                }
                // gitignore 대상이면 커밋되는 .example 이 있어야 한다 — 각자 복사해 쓴다.
                // 그 짝이 없으면 신규 클론은 만들 수 없는 파일을 마운트하게 된다.
                if (Files.exists(repo.resolve(source))
                        || Files.exists(repo.resolve(source + ".example"))) {
                    continue;
                }
                missing.add(entry.getKey() + " → " + source);
            }
        }

        assertThat(missing)
                .as("실측 — 없는 경로를 bind mount 하면 Docker 가 그 이름의 디렉터리를 만들어"
                        + " 마운트한다. 파일인 줄 알고 읽는 쪽은 설정이 통째로 비거나"
                        + " 크래시루프에 빠지는데, 에러 메시지에는 그 원인이 안 나온다."
                        + " 커밋 대상이 아니면 <경로>.example 을 커밋해 둘 것")
                .isEmpty();
    }

    @Test
    @DisplayName("TSDB 는 named volume 위에 있다 — 컨테이너를 지워도 과거 회차가 남아야 한다")
    void theTsdbLivesOnANamedVolumeThatTheCommandActuallyWritesTo() throws IOException {
        Map<String, Object> compose = loadYaml(repoRoot().resolve(COMPOSE_FILE));
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
                .as("--config.file 과 file_sd_configs 가 읽는 설정·target 경로가 실제로 마운트돼"
                        + " 있어야 한다. 한쪽만 고치면 기본 설정으로 뜨거나 게이트웨이를 못 찾는다")
                .contains(
                        "./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro",
                        "./infra/prometheus/targets:/etc/prometheus/targets:ro");

        assertThat(configFile)
                .as("명시적으로 마운트한 실제 설정 파일을 가리켜야 한다")
                .isEqualTo("/etc/prometheus/prometheus.yml");
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
            String jobName = String.valueOf(job.get("job_name"));
            if (job.containsKey("static_configs")) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> statics =
                        (List<Map<String, Object>>) job.get("static_configs");
                byJob.put(jobName, stringList(statics.get(0).get("targets")).get(0));
                continue;
            }

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> fileSd =
                    (List<Map<String, Object>>) job.get("file_sd_configs");
            assertThat(fileSd)
                    .as("%s — static target과 file_sd 둘 중 하나는 필요하다", jobName)
                    .isNotEmpty();
            byJob.put(jobName, stringList(fileSd.get(0).get("files")).get(0));
        }
        return byJob;
    }

    private Map<String, Object> jobNamed(Map<String, Object> prometheus, String name) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobs =
                (List<Map<String, Object>>) prometheus.get("scrape_configs");
        return jobs.stream()
                .filter(job -> name.equals(String.valueOf(job.get("job_name"))))
                .findFirst()
                .orElseThrow(() -> new AssertionError("scrape job이 없다: " + name));
    }

    /**
     * {@code "호스트:컨테이너"} 매핑에서 <b>컨테이너 쪽</b> 포트만 뽑는다. 호스트 쪽은
     * {@code ${VAR}} 라 값을 모르지만, 무엇이 열리는지는 컨테이너 쪽이 정한다.
     *
     * <p>{@code ports} 키가 아예 없으면 빈 목록이다 — 그게 "아무것도 안 연다" 의 표현이다.
     */
    private List<String> publishedContainerPorts(String service) throws IOException {
        Map<String, Object> definition = serviceNamed(service);
        Object ports = definition.get("ports");
        if (ports == null) {
            return List.of();
        }
        // ⚠️ 마지막 ':' 로 자르지 않는다. OBS-35 에서 컨테이너 쪽이 ${SERVER_PORT:-8080} 가
        //    되면서 매핑 안에 콜론이 더 생겼다 — 그렇게 자르면 "-8080}" 이 나오고, 이 단언은
        //    "8080 을 연다" 를 확인하지 못한 채 빨간불이 된다(실측).
        return stringList(ports).stream()
                .map(mapping -> {
                    List<String> sides = ComposePortMapping.split(mapping);
                    return ComposePortMapping.defaultOfFragment(sides.get(sides.size() - 1));
                })
                .toList();
    }

    private Map<String, Object> serviceNamed(String service) throws IOException {
        Map<String, Object> compose = loadYaml(repoRoot().resolve(COMPOSE_FILE));
        @SuppressWarnings("unchecked")
        Map<String, Object> services = (Map<String, Object>) compose.get("services");
        @SuppressWarnings("unchecked")
        Map<String, Object> definition = (Map<String, Object>) services.get(service);
        assertThat(definition).as("compose 에 %s 서비스가 없다", service).isNotNull();
        return definition;
    }

    private Map<String, Object> prometheusService() throws IOException {
        Map<String, Object> compose = loadYaml(repoRoot().resolve(COMPOSE_FILE));
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

    /** {@code 30d} 같은 보관 기간을 일 수로 바꾼다. 다른 단위는 읽지 않는다 — 여기서 멈춘다. */
    private long days(String retention) {
        if (retention.endsWith("d")) {
            return Long.parseLong(retention.substring(0, retention.length() - 1));
        }
        throw new AssertionError("모르는 보관 기간 단위: " + retention
                + " — 일(d) 단위로 쓰거나 여기를 넓혀라");
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

}
