package com.kafkick.storage.db.coupon.repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StorageCouponArchitectureTest {

    private static final List<String> OPERATION_SPECIFIC_FAILURES = List.of(
            "CouponUsePersistenceException",
            "CouponCancelUsePersistenceException",
            "CouponCancelPersistenceException",
            "CouponExpirationPersistenceException",
            "CouponIssuePersistenceException",
            "CouponQueryPersistenceException",
            "CouponRoundPersistenceException",
            "CouponStockLockPersistenceException",
            "CouponStockReleasePersistenceException"
    );

    @Test
    void issuancePersistenceDoesNotChooseBusinessErrorByStatusOrEvent()
            throws IOException {
        Path repositoryDirectory = Path.of(
                "src", "main", "java", "com", "kafkick", "storage",
                "db", "coupon", "repository"
        );
        try (var sources = Files.walk(repositoryDirectory)) {
            for (Path sourcePath : sources
                    .filter(path -> path.toString().endsWith(".java"))
                    .toList()) {
                String source = Files.readString(sourcePath);
                assertThat(OPERATION_SPECIFIC_FAILURES)
                        .as("storage repository must use a common persistence "
                                + "exception: %s", sourcePath)
                        .noneMatch(source::contains);
            }
        }
    }
}
