package com.kafkick.api.admin.benchmark;

import java.time.Duration;
import java.time.Instant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import com.kafkick.core.benchmark.BenchmarkErrorCode;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.observation.EngineVersion;

/** compose 내부 업무 포트로 batch의 FINAL 계산을 호출합니다. */
@Component
public class HttpBatchConsistencyFinalClient implements BatchConsistencyFinalClient {
    private final RestClient client;

    public HttpBatchConsistencyFinalClient(
            @Value("${benchmark.batch.base-url:http://batch:9091}") String baseUrl,
            @Value("${benchmark.batch.connect-timeout:100ms}") Duration connectTimeout,
            @Value("${benchmark.batch.consistency-read-timeout:3s}") Duration readTimeout) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeout);
        factory.setReadTimeout(readTimeout);
        this.client = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }

    @Override
    public ConsistencyEvaluation evaluate(long couponId, EngineVersion engineVersion,
                                          Instant runFinalizedAt) {
        BatchConsistencyFinalResponse result;
        try {
            result = client.post()
                    .uri(uriBuilder -> uriBuilder
                            .path("/internal/v1/benchmarks/consistency/final")
                            .queryParam("couponId", couponId)
                            .queryParam("engineVersion", engineVersion.name())
                            .queryParam("runFinalizedAt", runFinalizedAt.toString())
                            .build())
                    .retrieve()
                    .body(BatchConsistencyFinalResponse.class);
        } catch (RestClientResponseException failure) {
            // 상태코드만으로는 재실행 판단이 안 되므로 batch가 실은 원인 본문을 detail 로 옮긴다.
            // 503 은 원천이 돌아오면 복구되고, 나머지는 다시 눌러도 같은 답이라 구분해서 낸다.
            boolean unavailable = failure.getStatusCode().value()
                    == HttpStatus.SERVICE_UNAVAILABLE.value();
            throw new BusinessException(
                    unavailable ? BenchmarkErrorCode.CONSISTENCY_SOURCE_UNAVAILABLE
                            : BenchmarkErrorCode.CONSISTENCY_BATCH_REJECTED,
                    "batch FINAL 계산 거절 " + failure.getStatusCode().value() + " "
                            + failure.getResponseBodyAsString(), failure);
        } catch (RestClientException failure) {
            throw new BusinessException(BenchmarkErrorCode.CONSISTENCY_SOURCE_UNAVAILABLE,
                    "batch FINAL 계산 호출 실패 " + failure.getClass().getSimpleName() + ": "
                            + failure.getMessage(), failure);
        }
        if (result == null || result.evaluation() == null) {
            throw new BusinessException(BenchmarkErrorCode.CONSISTENCY_SOURCE_UNAVAILABLE,
                    "batch FINAL 계산 응답이 비었습니다");
        }
        return result.evaluation();
    }
}
