package com.kafkick.core.observation;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class IssuanceFlowEventJsonTest {

    private static final IssuanceFlowEventFactory ENTRY_FACTORY = factory(
            "11111111-1111-1111-1111-111111111111"
    );
    private static final IssuanceFlowEventFactory ADMITTED_FACTORY = factory(
            "22222222-2222-2222-2222-222222222222"
    );
    private static final IssuanceFlowEventFactory ISSUE_FACTORY = factory(
            "33333333-3333-3333-3333-333333333333"
    );

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class));

    @Test
    void jackson3SerializationMatchesGoldenFixtures() {
        contextRunner.run(context -> {
            ObjectMapper objectMapper = context.getBean("jacksonJsonMapper", ObjectMapper.class);

            assertFixture(objectMapper, "entry-result.json", ENTRY_FACTORY.entry(
                    IssuanceFlowEventTest.context("request-1", false),
                    202,
                    null,
                    Dependency.NONE,
                    12L,
                    18L
            ));
            assertFixture(objectMapper, "queue-admitted.json", ADMITTED_FACTORY.admitted(
                    IssuanceFlowEventTest.context(null, false),
                    18L
            ));
            assertFixture(objectMapper, "issue-result.json", ISSUE_FACTORY.issued(
                    IssuanceFlowEventTest.context("request-2", false),
                    101L,
                    "ISSUANCE0000001"
            ));
        });
    }

    @Test
    void omitsGradeWhenFailureOccursBeforeGradeIsKnown() {
        contextRunner.run(context -> {
            ObjectMapper objectMapper = context.getBean("jacksonJsonMapper", ObjectMapper.class);
            IssuanceFlowEvent event = ISSUE_FACTORY.issueRejected(
                    IssuanceFlowEventTest.context("request-3", false, null),
                    503,
                    ReasonCode.TEMPORARILY_UNAVAILABLE,
                    Dependency.MYSQL
            );

            JsonNode json = objectMapper.valueToTree(event);

            assertThat(json.has("grade")).isFalse();
        });
    }

    private static void assertFixture(
            ObjectMapper objectMapper,
            String fixtureName,
            IssuanceFlowEvent event
    ) {
        try {
            JsonNode actual = objectMapper.valueToTree(event);
            JsonNode expected = objectMapper.readTree(readFixture(fixtureName));
            assertThat(actual.equals(IssuanceFlowEventJsonTest::compareJsonValues, expected)).isTrue();
        } catch (IOException exception) {
            throw new AssertionError("fixture를 읽을 수 없습니다: " + fixtureName, exception);
        }
    }

    private static int compareJsonValues(JsonNode left, JsonNode right) {
        if (left.isNumber() && right.isNumber()) {
            return left.decimalValue().compareTo(right.decimalValue());
        }
        return left.equals(right) ? 0 : 1;
    }

    private static IssuanceFlowEventFactory factory(String eventId) {
        return new IssuanceFlowEventFactory(() -> java.util.UUID.fromString(eventId));
    }

    private static String readFixture(String fixtureName) throws IOException {
        String path = "/fixtures/obs1/" + fixtureName;
        try (var input = IssuanceFlowEventJsonTest.class.getResourceAsStream(path)) {
            if (input == null) {
                throw new IOException("fixture가 없습니다: " + path);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
