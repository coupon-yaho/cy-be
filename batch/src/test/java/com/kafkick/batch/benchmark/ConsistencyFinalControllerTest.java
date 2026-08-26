package com.kafkick.batch.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.EnumMap;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.kafkick.batch.benchmark.ConsistencyFinalController.MismatchResponse;

import com.kafkick.batch.observation.ConsistencyRawValueReader;
import com.kafkick.batch.observation.ConsistencyRawValueReader.DomainRawSnapshot;
import com.kafkick.batch.observation.DomainGaugeProperties;
import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.ConsistencyErrorCode;
import com.kafkick.core.consistency.ConsistencyRawSnapshot;
import com.kafkick.core.consistency.SourceObservation;
import com.kafkick.core.consistency.GapValue;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.exception.BusinessException;

class ConsistencyFinalControllerTest {
    private final ConsistencyRawValueReader reader = mock(ConsistencyRawValueReader.class);
    private final ConsistencyCalculator calculator = mock(ConsistencyCalculator.class);
    private final ConsistencyFinalController controller = new ConsistencyFinalController(
            reader, calculator,
            new DomainGaugeProperties(EngineVersion.V3, 7L, null, null, null, null, null));

    @Test
    void liveReaderSnapshotIsPassedToCalculatorWithFinalPhase() {
        ConsistencyRawSnapshot raw = mock(ConsistencyRawSnapshot.class);
        DomainRawSnapshot snapshot = new DomainRawSnapshot(7L, raw, null, SourceStatus.PENDING);
        ConsistencyEvaluation expected = evaluation();
        when(reader.read()).thenReturn(snapshot);
        when(calculator.evaluate(raw, ConsistencyPhase.FINAL, EngineVersion.V3)).thenReturn(expected);

        var response = controller.evaluate(7L, EngineVersion.V3);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isSameAs(expected);
        verify(reader).read();
        verify(calculator).evaluate(raw, ConsistencyPhase.FINAL, EngineVersion.V3);
    }

    @Test
    void differentGaugeCouponIsRejectedInsteadOfSavingAnotherCampaign() {
        when(reader.read()).thenReturn(new DomainRawSnapshot(
                8L, mock(ConsistencyRawSnapshot.class), null, SourceStatus.PENDING));
        var response = controller.evaluate(7L, EngineVersion.V3);
        // 500이면 api가 원인을 저장할 수 없다. 409 본문에 기대·실제 회차가 남아야 한다.
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(((MismatchResponse) response.getBody()).violations())
                .singleElement()
                .satisfies(violation -> {
                    assertThat(violation.key()).contains("coupon-id");
                    assertThat(violation.expected()).isEqualTo("7");
                    assertThat(violation.actual()).isEqualTo("8");
                });
    }

    @Test
    void differentGaugeEngineIsRejectedBeforeReadingMixedSemantics() {
        var response = controller.evaluate(7L, EngineVersion.V2);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(((MismatchResponse) response.getBody()).violations())
                .singleElement()
                .satisfies(violation -> {
                    assertThat(violation.key()).contains("engine-version");
                    assertThat(violation.expected()).isEqualTo("V2");
                    assertThat(violation.actual()).isEqualTo("V3");
                });
        org.mockito.Mockito.verifyNoInteractions(reader);
    }

    @Test
    void unavailableSourceIsReportedAsServiceUnavailableWithItsSourceStates() {
        ConsistencyRawSnapshot raw = rawSnapshot();
        when(reader.read()).thenReturn(new DomainRawSnapshot(7L, raw, null, SourceStatus.PENDING));
        when(calculator.evaluate(raw, ConsistencyPhase.FINAL, EngineVersion.V3))
                .thenThrow(new BusinessException(ConsistencyErrorCode.FINAL_VALUE_UNAVAILABLE,
                        "FINAL 평가에 필요한 gap이 유효하지 않습니다: LUA_GAP, state=STALE"));

        var response = controller.evaluate(7L, EngineVersion.V3);

        // 500 + 빈 본문이면 consistency_failure_reason 이 재실행 판단 근거가 되지 못한다.
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(((MismatchResponse) response.getBody()).violations())
                .singleElement()
                .satisfies(violation -> {
                    assertThat(violation.actual()).isEqualTo("redis=UNAVAILABLE,db=VALID");
                    assertThat(violation.reason()).contains("LUA_GAP", "state=STALE");
                });
    }

    @Test
    void unusableSourceStateIsReportedAsUnprocessableWithItsCause() {
        ConsistencyRawSnapshot raw = rawSnapshot();
        when(reader.read()).thenReturn(new DomainRawSnapshot(7L, raw, null, SourceStatus.PENDING));
        when(calculator.evaluate(raw, ConsistencyPhase.FINAL, EngineVersion.V3))
                .thenThrow(new BusinessException(ConsistencyErrorCode.INVALID_SOURCE_STATE,
                        "정합성 계산에 사용할 수 없는 원천 상태입니다: N_A"));

        var response = controller.evaluate(7L, EngineVersion.V3);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(((MismatchResponse) response.getBody()).violations())
                .singleElement()
                .satisfies(violation -> assertThat(violation.reason()).contains("N_A"));
    }

    @Test
    void observationPoolReadFailureIsReportedInsteadOfABodylessFiveHundred() {
        when(reader.read()).thenThrow(
                new org.springframework.dao.QueryTimeoutException("max_execution_time"));

        var response = controller.evaluate(7L, EngineVersion.V3);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(((MismatchResponse) response.getBody()).violations())
                .singleElement()
                .satisfies(violation ->
                        assertThat(violation.reason()).contains("QueryTimeoutException"));
    }

    @Test
    void httpParametersAndResponseAreBoundByTheActualMvcConversionService() throws Exception {
        ConsistencyRawSnapshot raw = rawSnapshot();
        when(reader.read()).thenReturn(new DomainRawSnapshot(7L, raw, null, SourceStatus.PENDING));
        when(calculator.evaluate(raw, ConsistencyPhase.FINAL, EngineVersion.V3))
                .thenReturn(evaluation());

        // 직접 호출 테스트는 문자열 -> Instant 변환 경로를 한 번도 태우지 않는다.
        MockMvcBuilders.standaloneSetup(controller).build()
                .perform(MockMvcRequestBuilders.post("/internal/v1/benchmarks/consistency/final")
                        .param("couponId", "7")
                        .param("engineVersion", "V3"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                // api 클라이언트가 파싱하는 키 모양을 여기서 고정한다. 양쪽이 각자 mock 이면
                // 직렬화가 갈라져도 어느 테스트도 깨지지 않는다.
                .andExpect(MockMvcResultMatchers.jsonPath("$.phase").value("FINAL"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.verdict").value("PASS"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.gaps.LUA_GAP.state").value("VALID"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.overIssued.observedAt").exists());
    }

    private static ConsistencyRawSnapshot rawSnapshot() {
        return new ConsistencyRawSnapshot(
                mock(com.kafkick.core.consistency.ConsistencyRawValues.class),
                new SourceObservation(SourceStatus.UNAVAILABLE, null),
                new SourceObservation(SourceStatus.VALID, Instant.EPOCH));
    }

    private static ConsistencyEvaluation evaluation() {
        var gaps = new EnumMap<ConsistencyGapType, GapValue>(ConsistencyGapType.class);
        for (ConsistencyGapType type : ConsistencyGapType.values()) {
            gaps.put(type, new GapValue(0L, SourceStatus.VALID, Instant.EPOCH));
        }
        return new ConsistencyEvaluation(gaps,
                new GapValue(0L, SourceStatus.VALID, Instant.EPOCH),
                ConsistencyPhase.FINAL, Verdict.PASS, Severity.NONE);
    }
}
