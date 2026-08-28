package com.kafkick.api.admin.benchmark;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

/** compose 내부 업무 포트로 batch 로컬 preflight를 조회한다. */
@Component
public class HttpBatchTopologyPreflight implements BatchTopologyPreflight {

    private final RestClient client;

    public HttpBatchTopologyPreflight(
        @Value("${benchmark.batch.base-url:http://batch:9091}") String baseUrl,
        @Value("${benchmark.batch.connect-timeout:100ms}") Duration connectTimeout,
        @Value("${benchmark.batch.read-timeout:300ms}") Duration readTimeout
    ) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(connectTimeout);
        requestFactory.setReadTimeout(readTimeout);
        this.client = RestClient.builder()
            .baseUrl(baseUrl)
            .requestFactory(requestFactory)
            .build();
    }

    @Override
    public Result validate(long couponId) {
        try {
            return client.get()
                .uri(uriBuilder -> uriBuilder.path("/internal/v1/benchmarks/preflight")
                    .queryParam("couponId", couponId).build())
                .retrieve()
                .onStatus(status -> status.value() == 409, (request, response) -> { })
                .body(Result.class);
        } catch (RestClientResponseException responseFailure) {
            return new Result(false, java.util.List.of(new Violation(
                "batch.preflight", "HTTP 200 or 409",
                "HTTP " + responseFailure.getStatusCode().value(),
                responseFailure.getClass().getSimpleName())));
        } catch (RestClientException | IllegalArgumentException failure) {
            return new Result(false, java.util.List.of(new Violation(
                "batch.preflight", "reachable within timeout", "unavailable",
                failure.getClass().getSimpleName())));
        }
    }
}
