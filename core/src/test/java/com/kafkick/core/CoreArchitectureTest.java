package com.kafkick.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class CoreArchitectureTest {

    private static final List<String> FORBIDDEN_IMPORTS = List.of(
            "import jakarta.persistence.",
            // ⚠️ 애노테이션은 뺀다. core/build.gradle 이 jackson-annotations 를 명시적으로
            //    의존하고(implementation), IssuanceFlowEvent 가 @JsonInclude 로 직렬화 계약을
            //    선언한다. 막아야 하는 것은 "core 가 매퍼를 안다" 이지 "core 가 계약을 적는다" 가
            //    아니다 — 애노테이션은 core 가 소유한 이벤트 스키마의 일부다.
            //
            //    금지가 살아 있는 쪽은 databind·core 다. 그쪽을 물면 core 가 직렬화 구현에
            //    묶이고, 매퍼 설정이 어댑터에서 core 로 새어 들어온다.
            "import com.fasterxml.jackson.databind.",
            "import com.fasterxml.jackson.core.",
            "import tools.jackson.",
            "import org.springframework.data.jpa.",
            "import org.springframework.kafka.",
            "import org.springframework.data.redis.",
            "import com.kafkick.api.",
            "import com.kafkick.batch.",
            "import com.kafkick.storage.",
            "JpaRepository",
            "KafkaTemplate",
            "RedisTemplate"
    );

    @Test
    void coreDoesNotDependOnAdapterTypes() throws IOException {
        Path sourceRoot = Path.of("src", "main", "java");

        List<String> violations;
        try (Stream<Path> paths = Files.walk(sourceRoot)) {
            violations = paths
                    .filter(path -> path.toString().endsWith(".java"))
                    .flatMap(CoreArchitectureTest::forbiddenImports)
                    .toList();
        }

        assertThat(violations).isEmpty();
    }

    private static Stream<String> forbiddenImports(Path path) {
        try {
            String source = Files.readString(path);
            return FORBIDDEN_IMPORTS.stream()
                    .filter(source::contains)
                    .map(forbidden -> path + ": " + forbidden);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "core 소스 파일을 읽을 수 없습니다: " + path,
                    exception
            );
        }
    }
}
