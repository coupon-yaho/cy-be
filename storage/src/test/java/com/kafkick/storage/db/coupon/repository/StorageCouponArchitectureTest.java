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
            "CouponExpirationPersistenceException"
    );

    @Test
    void issuancePersistenceDoesNotChooseBusinessErrorByStatusOrEvent()
            throws IOException {
        for (String fileName : List.of(
                "IssuanceRepositoryImpl.java",
                "IssuanceHistoryRepositoryImpl.java"
        )) {
            String source = Files.readString(Path.of(
                    "src", "main", "java", "com", "kafkick", "storage",
                    "db", "coupon", "repository", fileName
            ));
            assertThat(OPERATION_SPECIFIC_FAILURES)
                    .noneMatch(source::contains);
        }
    }
}
