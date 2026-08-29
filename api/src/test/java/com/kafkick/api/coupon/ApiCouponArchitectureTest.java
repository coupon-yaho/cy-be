package com.kafkick.api.coupon;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ApiCouponArchitectureTest {

    private static final List<String> FORBIDDEN_COUPON_DEPENDENCIES = List.of(
            "import org.springframework.transaction.annotation.Transactional;",
            "import com.kafkick.core.coupon.port.IdempotencyRepository;",
            "import com.kafkick.storage."
    );

    @Test
    void couponApiOnlyHandlesHttpMappingAndDtoConversion() throws IOException {
        List<String> violations = new java.util.ArrayList<>();
        for (String domainPackage : List.of("coupon", "coupontemplate")) {
            Path sourceRoot = Path.of(
                    "src", "main", "java", "com", "kafkick", "api",
                    domainPackage
            );
            try (Stream<Path> paths = Files.walk(sourceRoot)) {
                violations.addAll(paths
                        .filter(path -> path.toString().endsWith(".java"))
                        .filter(path -> !path.toString().contains(
                                java.io.File.separator + "config"
                                        + java.io.File.separator
                        ))
                        .flatMap(ApiCouponArchitectureTest::findViolations)
                        .toList());
            }
        }

        assertThat(violations).isEmpty();
    }

    private static Stream<String> findViolations(Path path) {
        try {
            String source = Files.readString(path);
            return FORBIDDEN_COUPON_DEPENDENCIES.stream()
                    .filter(source::contains)
                    .map(forbidden -> path + ": " + forbidden);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "API 쿠폰 소스 파일을 읽을 수 없습니다: " + path,
                    exception
            );
        }
    }
}
