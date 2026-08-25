package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import com.kafkick.infra.mq.attempt.AttemptSamplingProperties;

/**
 * 층화 샘플링 계약이 <b>두 파일에 걸쳐 있다.</b>
 *
 * <p>기본값 상수는 {@code infra:mq} 의 {@link AttemptSamplingProperties} 가 갖고, 실제로 로드되는
 * 키는 이 모듈의 {@code observation.yml} 이 갖는다. 각각을 따로 보는 테스트 두 개로는 계약을
 * 지키지 못한다 — 한쪽만 바꿔도 양쪽 테스트가 전부 초록이고, 예외도 로그도 없이 통과량만
 * 조용히 달라진다. 그 차이는 부하 회차의 화면에서만 드러나고, 그때는 이미 늦다.
 *
 * <p>키 <b>존재</b>도 함께 본다. 값만 비교하면 키 이름의 오타를 못 잡는다 — 오타가 나면 Boot 가
 * 그 키를 무시하고 record 의 {@code null} 분기가 기본값을 채우므로, 설정 파일에 100 이라고
 * 적혀 있는데 실제로는 상수의 100 이 쓰이는 상태가 된다. 두 값이 같은 동안에는 아무도 모르고,
 * 값을 바꾸는 날 "고쳤는데 안 바뀐다" 가 된다.
 */
class AttemptSamplingYamlContractTest {

    private static final String PREFIX_PATH = "observation.attempt.live.sampling";

    @Test
    void ymlDefaultsMatchThePropertyConstants() throws IOException {
        Map<String, Object> sampling = samplingSection();

        assertThat(sampling).containsOnlyKeys(
                "min-per-stratum-per-second", "max-per-second", "max-strata");
        assertThat(defaultOf(sampling, "min-per-stratum-per-second"))
                .isEqualTo(AttemptSamplingProperties.DEFAULT_MIN_PER_STRATUM_PER_SECOND);
        assertThat(defaultOf(sampling, "max-per-second"))
                .isEqualTo(AttemptSamplingProperties.DEFAULT_MAX_PER_SECOND);
        assertThat(defaultOf(sampling, "max-strata"))
                .isEqualTo(AttemptSamplingProperties.DEFAULT_MAX_STRATA);
    }

    /**
     * yml 의 기본값을 그대로 넣은 것과 아무것도 안 넣은 것이 같은 설정이어야 한다.
     *
     * <p>위 테스트는 문자열 대조라 "두 숫자가 같다" 까지만 본다. 이 테스트는 그 값들이 실제로
     * 같은 {@code AttemptSamplingProperties} 를 만드는지를 본다 — 상수를 딴 데 쓰는 리팩터링이
     * 들어와도 계약이 유지된다.
     */
    @Test
    void ymlDefaultsProduceTheSamePropertiesAsOmittingThem() throws IOException {
        Map<String, Object> sampling = samplingSection();

        AttemptSamplingProperties fromYml = new AttemptSamplingProperties(
                defaultOf(sampling, "min-per-stratum-per-second"),
                defaultOf(sampling, "max-per-second"),
                defaultOf(sampling, "max-strata"));
        AttemptSamplingProperties fromConstants = new AttemptSamplingProperties(null, null, null);

        assertThat(fromYml).isEqualTo(fromConstants);
    }

    /**
     * {@code ${VAR:default}} 에서 기본값만 뽑는다.
     *
     * <p>환경 변수 이름은 일부러 안 본다 — 그것은 배포 계약이고 이 테스트가 지키려는 것은
     * "코드 상수와 설정 기본값이 같다" 하나다. 둘을 한 테스트에 넣으면 배포 변수 이름을 바꿀
     * 때마다 이 테스트가 깨져서, 진짜 계약이 깨졌을 때와 구분이 안 된다.
     */
    private static int defaultOf(Map<String, Object> sampling, String key) {
        String raw = String.valueOf(sampling.get(key));
        int colon = raw.lastIndexOf(':');
        int close = raw.lastIndexOf('}');
        if (colon < 0 || close < 0) {
            throw new AssertionError(PREFIX_PATH + "." + key + " 가 ${VAR:default} 형식이 아니다: " + raw);
        }
        return Integer.parseInt(raw.substring(colon + 1, close).trim());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> samplingSection() throws IOException {
        // 커밋되는 것은 .example 이다. 실행용 observation.yml 은 gitignore 대상이라 갓 클론한
        // 환경에는 없다 — 그것을 읽으면 이 가드가 로컬 복사본에만 걸린다.
        Path example = Path.of("src/main/resources/observation.yml.example");
        Map<String, Object> root = new Yaml().load(
                Files.readString(example, StandardCharsets.UTF_8));
        Object node = root;
        for (String segment : PREFIX_PATH.split("\\.")) {
            node = ((Map<String, Object>) node).get(segment);
            if (node == null) {
                throw new AssertionError(PREFIX_PATH + " 절이 observation.yml.example 에 없다");
            }
        }
        return (Map<String, Object>) node;
    }
}
