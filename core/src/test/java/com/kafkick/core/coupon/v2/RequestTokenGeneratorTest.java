package com.kafkick.core.coupon.v2;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestTokenGeneratorTest {

    @Test
    void includesInstanceIdThreadIdAndCounter() {
        RequestTokenGenerator generator = new RequestTokenGenerator("api-a", "boot-a", 10L);
        long threadId = Thread.currentThread().threadId();

        assertThat(generator.generate()).isEqualTo("api-a-boot-a-" + threadId + "-10");
        assertThat(generator.generate()).isEqualTo("api-a-boot-a-" + threadId + "-11");
    }

    @Test
    void differentBootsWithSameInstanceIdProduceDisjointTokens() {
        RequestTokenGenerator first = new RequestTokenGenerator("api-a", "boot-a", 0L);
        RequestTokenGenerator second = new RequestTokenGenerator("api-a", "boot-b", 0L);
        Set<String> firstTokens = generate(first, 1_000);
        Set<String> secondTokens = generate(second, 1_000);

        assertThat(firstTokens).doesNotContainAnyElementsOf(secondTokens);
    }

    @Test
    void publicGeneratorsWithSameInstanceIdUseDifferentBootNonces() {
        Set<String> firstTokens = generate(new RequestTokenGenerator("api-a"), 1_000);
        Set<String> secondTokens = generate(new RequestTokenGenerator("api-a"), 1_000);

        assertThat(firstTokens).doesNotContainAnyElementsOf(secondTokens);
    }

    @Test
    void rejectsInstanceIdContainingDelimiter() {
        assertThatThrownBy(() -> new RequestTokenGenerator("api|a"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void producesUniqueTokensAcrossThreads() throws Exception {
        RequestTokenGenerator generator = new RequestTokenGenerator("api-a", "boot-a", 0L);
        Set<String> tokens = ConcurrentHashMap.newKeySet();
        int taskCount = 1_000;

        try (ExecutorService executor = Executors.newFixedThreadPool(8)) {
            for (int index = 0; index < taskCount; index++) {
                executor.submit(() -> tokens.add(generator.generate()));
            }
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(tokens).hasSize(taskCount);
    }

    private static Set<String> generate(RequestTokenGenerator generator, int count) {
        Set<String> tokens = new HashSet<>();
        for (int index = 0; index < count; index++) {
            tokens.add(generator.generate());
        }
        return tokens;
    }
}
