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
            "import com.fasterxml.jackson.",
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
