package com.kafkick.api.admin.benchmark;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import com.kafkick.core.consistency.ConsistencyEvaluation;
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
    public ConsistencyEvaluation evaluate(long couponId, EngineVersion engineVersion) {
        ConsistencyEvaluation result;
        try {
            result = client.post()
                    .uri(uriBuilder -> uriBuilder
                            .path("/internal/v1/benchmarks/consistency/final")
                            .queryParam("couponId", couponId)
                            .queryParam("engineVersion", engineVersion.name())
                            .build())
                    .retrieve()
                    .body(ConsistencyEvaluation.class);
        } catch (RestClientResponseException failure) {
            // 상태코드만으로는 재실행 판단이 안 되므로 batch가 실은 원인 본문을 그대로 옮긴다.
            throw new IllegalStateException("batch FINAL 계산 거절 "
                    + failure.getStatusCode().value() + " " + failure.getResponseBodyAsString(),
                    failure);
        } catch (RestClientException failure) {
            throw new IllegalStateException("batch FINAL 계산 호출 실패 "
                    + failure.getClass().getSimpleName() + ": " + failure.getMessage(), failure);
        }
        if (result == null) throw new IllegalStateException("batch FINAL 계산 응답이 비었습니다");
        return result;
    }
}
