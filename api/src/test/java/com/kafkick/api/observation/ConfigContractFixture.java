package com.kafkick.api.observation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

final class ConfigContractFixture {

    private ConfigContractFixture() {}

    static String defaultOf(String value) {
        if (!value.startsWith("${")) {
            return value;
        }
        int colon = value.lastIndexOf(':');
        if (colon < 0) {
            throw new AssertionError("기본값 없는 플레이스홀더라 대조할 값이 없다: " + value);
        }
        return value.substring(colon + 1, value.length() - 1);
    }

    static Map<String, Object> loadYaml(Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
            throw new AssertionError("계약에 걸린 파일이 없다: " + file);
        }
        try (var in = Files.newInputStream(file)) {
            var documents = new Yaml().loadAll(in).iterator();
            if (!documents.hasNext()) {
                throw new AssertionError("비어 있는 YAML 파일이다: " + file);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> first = (Map<String, Object>) documents.next();
            return first;
        }
    }

    static Path repoRoot() {
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
