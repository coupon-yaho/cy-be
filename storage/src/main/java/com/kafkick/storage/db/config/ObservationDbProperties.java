package com.kafkick.storage.db.config;

import java.time.Duration;

import jakarta.validation.constraints.NotBlank;

import org.hibernate.validator.constraints.time.DurationMin;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * 관측 전용 DataSource 접속 정보. 운영 계정과 분리해 읽기 전용 계정을 주입할 수 있게 별도 키로 둔다.
 *
 * <p>storage.yml 은 커밋하지 않으므로 낡은 파일을 받은 사람에게 이 블록이 통째로 없을 수 있다.
 * 검증이 없으면 전부 null 로 바인딩되고, Hikari 는 첫 조회까지 풀을 열지 않아 헬스체크·롤아웃이
 * 다 끝난 뒤 대시보드 첫 요청에서 죽는다. 기동 시점에 죽이는 편이 낫다.
 */
@Validated
@ConfigurationProperties("observation.datasource")
public record ObservationDbProperties(
    String driverClassName,
    @NotBlank String url,
    @NotBlank String username,
    @NotBlank String password,
    @DurationMin(seconds = 1) Duration queryTimeout
) {

    private static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofSeconds(3);

    /**
     * 접속 정보와 달리 이건 안 적어도 기동을 막을 이유가 없다. 다만 무제한은 안 된다.
     *
     * <p>1초 미만을 금지하는 이유 — JDBC 의 쿼리 타임아웃은 초 단위라 1초 미만은 0으로 잘리고,
     * JDBC 에서 0 은 "제한 없음" 이다. 조이려던 설정이 푸는 설정으로 뒤집힌다.
     */
    public ObservationDbProperties {
        queryTimeout = queryTimeout != null ? queryTimeout : DEFAULT_QUERY_TIMEOUT;
    }
}
