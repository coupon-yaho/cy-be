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
        Path storageDbDirectory = Path.of(
                "src", "main", "java", "com", "kafkick", "storage",
                "db"
        );
        try (var sources = Files.walk(storageDbDirectory)) {
            for (Path sourcePath : sources
                    .filter(StorageCouponArchitectureTest::isRepositorySource)
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

    private static boolean isRepositorySource(Path path) {
        return path.toString().contains(
                java.io.File.separator + "repository"
                        + java.io.File.separator
        );
    }
}
