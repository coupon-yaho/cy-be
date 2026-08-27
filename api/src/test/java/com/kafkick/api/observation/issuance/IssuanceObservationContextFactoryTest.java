package com.kafkick.api.observation.issuance;

import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.api.observation.ObservationIssuanceProperties;
import com.kafkick.core.member.Grade;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IssuanceObservationContextFactoryTest {

    private static final Instant OCCURRED_AT =
            Instant.parse("2026-08-24T05:30:00Z");

    @Mock
    private RuntimeConfigStore runtimeConfigStore;

    @Mock
    private TimeProvider timeProvider;

    private IssuanceObservationContextFactory factory;

    @BeforeEach
    void setUp() {
        factory = new IssuanceObservationContextFactory(
                runtimeConfigStore,
                timeProvider,
                new ObservationIssuanceProperties(null, "api-17")
        );
    }

    @Test
    void createsContextFromOneRuntimeConfigRead() {
        when(runtimeConfigStore.get()).thenReturn(snapshot(
                SourceStatus.VALID
        ));
        when(timeProvider.instant()).thenReturn(OCCURRED_AT);

        Optional<IssuanceFlowEvent.Ctx> result = factory.create(
                "request-1",
                20L,
                10L,
                MembershipGrade.GOLD
        );

        assertThat(result).contains(new IssuanceFlowEvent.Ctx(
                "request-1",
                20L,
                10L,
                Grade.GOLD,
                false,
                OCCURRED_AT,
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.ADAPTIVE,
                null,
                "api-17"
        ));
        verify(runtimeConfigStore).get();
        verify(runtimeConfigStore, never()).getLastKnownGood();
    }

    @ParameterizedTest
    @MethodSource("valueLessStatuses")
    void skipsObservationWhenRuntimeConfigStatusHasNoValue(
            SourceStatus status
    ) {
        when(runtimeConfigStore.get()).thenReturn(snapshot(status));

        Optional<IssuanceFlowEvent.Ctx> result = factory.create(
                "request-1",
                20L,
                10L,
                MembershipGrade.GOLD
        );

        assertThat(result).isEmpty();
        verify(runtimeConfigStore).get();
        verify(runtimeConfigStore, never()).getLastKnownGood();
        verify(timeProvider, never()).instant();
    }

    @ParameterizedTest
    @MethodSource("gradeMappings")
    void mapsEveryMembershipGradeExplicitly(
            MembershipGrade membershipGrade,
            Grade expectedGrade
    ) {
        when(runtimeConfigStore.get()).thenReturn(snapshot(
                SourceStatus.VALID
        ));
        when(timeProvider.instant()).thenReturn(OCCURRED_AT);

        IssuanceFlowEvent.Ctx context = factory.create(
                "request-1",
                20L,
                10L,
                membershipGrade
        ).orElseThrow();

        assertThat(context.grade()).isEqualTo(expectedGrade);
    }

    @Test
    void valueLessStatusesCoverEveryStatusThatCarriesNoValue() {
        assertThat(valueLessStatuses().toList())
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.stream(SourceStatus.values())
                                .filter(status -> !status.carriesValue())
                                .toList()
                );
    }

    @Test
    void gradeMappingsCoverEveryMembershipGrade() {
        assertThat(gradeMappings()
                .map(arguments ->
                        (MembershipGrade) arguments.get()[0]
                )
                .toList())
                .containsExactlyInAnyOrder(MembershipGrade.values());
    }

    private static Stream<SourceStatus> valueLessStatuses() {
        return Stream.of(
                SourceStatus.PENDING,
                SourceStatus.UNAVAILABLE,
                SourceStatus.N_A
        );
    }

    private static Stream<Arguments> gradeMappings() {
        return Stream.of(
                Arguments.of(MembershipGrade.WELCOME, Grade.WELCOME),
                Arguments.of(MembershipGrade.SILVER, Grade.SILVER),
                Arguments.of(MembershipGrade.GOLD, Grade.GOLD),
                Arguments.of(MembershipGrade.VIP, Grade.VIP)
        );
    }

    private static RuntimeConfigSnapshot snapshot(SourceStatus status) {
        return new RuntimeConfigSnapshot(
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.ADAPTIVE,
                7L,
                Instant.parse("2026-08-24T05:00:00Z"),
                "operator",
                status
        );
    }
}
