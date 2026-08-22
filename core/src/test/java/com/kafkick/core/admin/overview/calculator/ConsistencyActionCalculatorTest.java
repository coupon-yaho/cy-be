package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Instant;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.GapValue;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/** FINAL 정합성 판정을 운영자 조치 후보로 바꾸는 규칙을 검증합니다. */
class ConsistencyActionCalculatorTest {

    private static final Instant OPENS_AT = Instant.parse("2026-08-22T00:00:00Z");
    private static final Instant EVALUATED_AT = Instant.parse("2026-08-22T03:00:00Z");

    /** 정합성 실패 조치는 프론트 문구와 분리된 안정적인 서버 코드를 가져야 합니다. */
    @Test
    @DisplayName("정합성 실패 조치에는 명시적인 행동 코드가 있다")
    void exposesExplicitConsistencyFailureActionCode() {
        assertThat(AdminOverviewSnapshot.ActionCode.valueOf("CONSISTENCY_FAILURE"))
                .hasToString("CONSISTENCY_FAILURE");
    }

    /** FINAL 합격은 실제 0값을 정상으로 보존할 뿐 조치 후보를 만들지 않아야 합니다. */
    @Test
    @DisplayName("모든 FINAL 값이 유효하고 PASS이며 초과 발급이 0이면 조치가 없다")
    void returnsNoActionForFinalPassWithoutOverIssuance() {
        List<AdminOverviewSnapshot.OperationActionItem> result = new ConsistencyActionCalculator().calculate(
                context(finalEvaluation(Verdict.PASS, valid(0L), gaps(valid(0L)))));

        assertThat(result).isEmpty();
    }

    /** 일반 FINAL 불일치는 입력 식별자와 표시 문맥을 보존한 정합성 확인 조치가 되어야 합니다. */
    @Test
    @DisplayName("FINAL FAIL은 입력 문맥을 보존한 정합성 확인 조치를 만든다")
    void createsConsistencyActionForFinalFailure() {
        List<AdminOverviewSnapshot.OperationActionItem> result = new ConsistencyActionCalculator().calculate(
                context(finalEvaluation(Verdict.FAIL, valid(0L), gaps(valid(1L)))));

        assertThat(result).containsExactly(new AdminOverviewSnapshot.OperationActionItem(
                17L,
                "추석 선물 쿠폰",
                OPENS_AT,
                Severity.CRITICAL,
                AdminOverviewSnapshot.CustomerImpact.LIMITED,
                "정합성 불일치를 확인해야 합니다.",
                EVALUATED_AT,
                null,
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE,
                        "정합성 확인",
                        AdminOverviewSnapshot.TargetScreen.METRICS)));
    }

    /** 초과 발급은 일반 gap 불일치보다 고객 영향과 조치 문구에서 우선되어야 합니다. */
    @Test
    @DisplayName("초과 발급 FINAL은 고객 영향이 큰 최우선 정합성 조치를 만든다")
    void prioritizesOverIssuanceOverGeneralConsistencyGap() {
        List<AdminOverviewSnapshot.OperationActionItem> result = new ConsistencyActionCalculator().calculate(
                context(finalEvaluation(Verdict.FAIL, valid(3L), gaps(valid(7L)))));

        assertThat(result).containsExactly(new AdminOverviewSnapshot.OperationActionItem(
                17L,
                "추석 선물 쿠폰",
                OPENS_AT,
                Severity.CRITICAL,
                AdminOverviewSnapshot.CustomerImpact.WIDESPREAD,
                "초과 발급이 확인되었습니다.",
                EVALUATED_AT,
                null,
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE,
                        "초과 발급 확인",
                        AdminOverviewSnapshot.TargetScreen.METRICS)));
    }

    /** 진행 중 추세인 LIVE 평가를 최종 실패 조치로 잘못 읽는 회귀를 막습니다. */
    @Test
    @DisplayName("LIVE 정합성 결과는 조치 후보로 변환하지 않는다")
    void ignoresLiveConsistencyEvaluation() {
        ConsistencyEvaluation evaluation = new ConsistencyEvaluation(
                gaps(valid(9L)), valid(4L), ConsistencyPhase.LIVE, null, Severity.CRITICAL);

        assertThat(new ConsistencyActionCalculator().calculate(context(evaluation))).isEmpty();
    }

    /** FINAL 계산 불가 상태를 정상·0·조치 없음으로 축약하지 않는 명시적 거부 계약입니다. */
    @Test
    @DisplayName("계산 불가 FINAL gap은 조치 없음으로 축약하지 않고 거부한다")
    void rejectsUnavailableFinalGapInsteadOfReturningNoAction() {
        for (SourceStatus status : List.of(SourceStatus.PENDING, SourceStatus.UNAVAILABLE, SourceStatus.STALE)) {
            Map<ConsistencyGapType, GapValue> gaps = gaps(valid(0L));
            gaps.put(ConsistencyGapType.ACTIVE_DB_GAP, unavailable(status));
            ConsistencyEvaluation evaluation = finalEvaluation(Verdict.PASS, valid(0L), gaps);

            assertThatIllegalArgumentException().isThrownBy(() ->
                    new ConsistencyActionCalculator().calculate(context(evaluation)));
        }
    }

    /** `overIssued`를 알 수 없을 때도 정상 PASS나 빈 조치로 오해하면 안 됩니다. */
    @Test
    @DisplayName("계산 불가 FINAL 초과 발급 값은 조치 없음으로 축약하지 않고 거부한다")
    void rejectsUnavailableFinalOverIssuanceInsteadOfReturningNoAction() {
        ConsistencyEvaluation evaluation = finalEvaluation(
                Verdict.PASS, unavailable(SourceStatus.PENDING), gaps(valid(0L)));

        assertThatIllegalArgumentException().isThrownBy(() ->
                new ConsistencyActionCalculator().calculate(context(evaluation)));
    }

    /** `N_A` gap은 V1 FINAL 계산기가 만드는 비적용 값이므로 계산 불가로 취급하면 안 됩니다. */
    @Test
    @DisplayName("V1 FINAL의 N_A gap은 PASS 조치 없음으로 유지한다")
    void allowsNotApplicableGapsFromV1FinalEvaluation() {
        Map<ConsistencyGapType, GapValue> gaps = gaps(valid(0L));
        gaps.put(ConsistencyGapType.ACTIVE_DB_GAP, unavailable(SourceStatus.N_A));
        gaps.put(ConsistencyGapType.LUA_GAP, unavailable(SourceStatus.N_A));
        gaps.put(ConsistencyGapType.PERSIST_GAP, unavailable(SourceStatus.N_A));

        assertThat(new ConsistencyActionCalculator().calculate(
                context(EngineVersion.V1, finalEvaluation(Verdict.PASS, valid(0L), gaps)))).isEmpty();
    }

    /** 엔진 버전별로 계산기가 만드는 FINAL gap 적용 상태를 같은 계약으로 허용해야 합니다. */
    @Test
    @DisplayName("V1 V2 V3의 적용 gap 상태가 정상 FINAL 계약이면 PASS 조치가 없다")
    void acceptsEngineSpecificFinalGapContracts() {
        Map<ConsistencyGapType, GapValue> v1Gaps = gaps(valid(0L));
        v1Gaps.put(ConsistencyGapType.ACTIVE_DB_GAP, unavailable(SourceStatus.N_A));
        v1Gaps.put(ConsistencyGapType.LUA_GAP, unavailable(SourceStatus.N_A));
        v1Gaps.put(ConsistencyGapType.PERSIST_GAP, unavailable(SourceStatus.N_A));

        for (EngineVersion engineVersion : List.of(EngineVersion.V2, EngineVersion.V3)) {
            assertThat(new ConsistencyActionCalculator().calculate(
                    context(engineVersion, finalEvaluation(Verdict.PASS, valid(0L), gaps(valid(0L))))))
                    .isEmpty();
        }
        assertThat(new ConsistencyActionCalculator().calculate(
                context(EngineVersion.V1, finalEvaluation(Verdict.PASS, valid(0L), v1Gaps)))).isEmpty();
    }

    /** 적용 대상 gap을 N_A로 바꾸면 계산 불가 FINAL을 정상으로 축약할 수 없어야 합니다. */
    @Test
    @DisplayName("엔진 버전의 적용 대상 gap이 N_A이면 FINAL을 거부한다")
    void rejectsNotApplicableStateForEngineApplicableGap() {
        for (EngineVersion engineVersion : List.of(EngineVersion.V2, EngineVersion.V3)) {
            Map<ConsistencyGapType, GapValue> gaps = gaps(valid(0L));
            gaps.put(ConsistencyGapType.ACTIVE_DB_GAP, unavailable(SourceStatus.N_A));

            assertThatIllegalArgumentException().isThrownBy(() ->
                    new ConsistencyActionCalculator().calculate(
                            context(engineVersion, finalEvaluation(Verdict.PASS, valid(0L), gaps))));
        }
        Map<ConsistencyGapType, GapValue> v1Gaps = gaps(valid(0L));
        v1Gaps.put(ConsistencyGapType.DB_COUNTER_GAP, unavailable(SourceStatus.N_A));

        assertThatIllegalArgumentException().isThrownBy(() ->
                new ConsistencyActionCalculator().calculate(
                        context(EngineVersion.V1, finalEvaluation(Verdict.PASS, valid(0L), v1Gaps))));
    }

    /** FINAL의 확정 verdict는 엔진별 적용 수치와 초과 발급 수에서 다시 계산한 결과와 같아야 합니다. */
    @Test
    @DisplayName("FINAL verdict가 적용 gap 또는 초과 발급 수와 모순되면 거부한다")
    void rejectsFinalVerdictInconsistentWithApplicableValues() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new ConsistencyActionCalculator().calculate(context(finalEvaluation(
                        Verdict.PASS, valid(0L), gaps(valid(1L))))));
        assertThatIllegalArgumentException().isThrownBy(() ->
                new ConsistencyActionCalculator().calculate(context(finalEvaluation(
                        Verdict.FAIL, valid(0L), gaps(valid(0L))))));
        assertThatIllegalArgumentException().isThrownBy(() ->
                new ConsistencyActionCalculator().calculate(context(finalEvaluation(
                        Verdict.PASS, valid(1L), gaps(valid(0L))))));
    }

    /** 음수 초과 발급은 계산기 출력이 될 수 없으므로 조치 판정 전에 거부해야 합니다. */
    @Test
    @DisplayName("음수 overIssued FINAL은 거부한다")
    void rejectsNegativeOverIssuance() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new ConsistencyActionCalculator().calculate(context(finalEvaluation(
                        Verdict.FAIL, valid(-1L), gaps(valid(0L))))));
    }

    /** FINAL severity는 DefaultConsistencyCalculator가 확정하는 PASS NONE, FAIL CRITICAL 조합이어야 합니다. */
    @Test
    @DisplayName("FINAL severity가 verdict와 일관되지 않으면 거부한다")
    void rejectsFinalSeverityInconsistentWithVerdict() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new ConsistencyActionCalculator().calculate(context(finalEvaluation(
                        Verdict.PASS, Severity.CRITICAL, valid(0L), gaps(valid(0L))))));
        assertThatIllegalArgumentException().isThrownBy(() ->
                new ConsistencyActionCalculator().calculate(context(finalEvaluation(
                        Verdict.FAIL, Severity.NONE, valid(0L), gaps(valid(1L))))));
    }

    /** gap Map 생성 순서가 달라도 동일한 FINAL 실패 조치가 나와야 합니다. */
    @Test
    @DisplayName("FINAL gap 입력 순서와 무관하게 같은 정합성 조치를 만든다")
    void createsSameActionRegardlessOfFinalGapInputOrder() {
        Map<ConsistencyGapType, GapValue> forward = gaps(valid(1L));
        Map<ConsistencyGapType, GapValue> reversed = new LinkedHashMap<>();
        ConsistencyGapType[] gapTypes = ConsistencyGapType.values();
        for (int index = gapTypes.length - 1; index >= 0; index--) {
            reversed.put(gapTypes[index], valid(1L));
        }

        List<AdminOverviewSnapshot.OperationActionItem> forwardResult =
                new ConsistencyActionCalculator().calculate(
                        context(finalEvaluation(Verdict.FAIL, valid(0L), forward)));
        List<AdminOverviewSnapshot.OperationActionItem> reversedResult =
                new ConsistencyActionCalculator().calculate(
                        context(finalEvaluation(Verdict.FAIL, valid(0L), reversed)));

        assertThat(reversedResult).isEqualTo(forwardResult);
    }

    /** 순수 조치 계산에 필요한 FINAL 판정과 캠페인 표시 문맥을 함께 만듭니다. */
    private static ConsistencyActionContext context(ConsistencyEvaluation evaluation) {
        return context(EngineVersion.V2, evaluation);
    }

    /** 엔진별 FINAL 적용성 검증에 필요한 캠페인 문맥을 만듭니다. */
    private static ConsistencyActionContext context(
            EngineVersion engineVersion,
            ConsistencyEvaluation evaluation
    ) {
        return new ConsistencyActionContext(
                17L, "추석 선물 쿠폰", OPENS_AT, EVALUATED_AT, engineVersion, evaluation);
    }

    /** FINAL 단계가 가진 verdict와 severity를 실제 계산기 출력과 같은 조합으로 만듭니다. */
    private static ConsistencyEvaluation finalEvaluation(
            Verdict verdict,
            GapValue overIssued,
            Map<ConsistencyGapType, GapValue> gaps
    ) {
        return finalEvaluation(
                verdict,
                verdict == Verdict.FAIL ? Severity.CRITICAL : Severity.NONE,
                overIssued,
                gaps);
    }

    /** verdict와 severity의 모순을 포함한 직접 조립 FINAL 입력을 만듭니다. */
    private static ConsistencyEvaluation finalEvaluation(
            Verdict verdict,
            Severity severity,
            GapValue overIssued,
            Map<ConsistencyGapType, GapValue> gaps
    ) {
        return new ConsistencyEvaluation(
                gaps,
                overIssued,
                ConsistencyPhase.FINAL,
                verdict,
                severity);
    }

    /** 네 gap 축을 같은 유효 값으로 채운 가변 Map을 만듭니다. */
    private static Map<ConsistencyGapType, GapValue> gaps(GapValue value) {
        Map<ConsistencyGapType, GapValue> gaps = new EnumMap<>(ConsistencyGapType.class);
        for (ConsistencyGapType gapType : ConsistencyGapType.values()) {
            gaps.put(gapType, value);
        }
        return gaps;
    }

    /** 계산 완료한 FINAL 값을 만듭니다. */
    private static GapValue valid(long value) {
        return new GapValue(value, SourceStatus.VALID, EVALUATED_AT);
    }

    /** FINAL에서 거부하거나 비적용으로 보존할 값 없는 상태를 만듭니다. */
    private static GapValue unavailable(SourceStatus status) {
        if (status == SourceStatus.STALE) {
            return new GapValue(0L, status, EVALUATED_AT);
        }
        return new GapValue(null, status, null);
    }
}
