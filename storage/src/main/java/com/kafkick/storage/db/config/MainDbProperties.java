package com.kafkick.storage.db.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 운영 DataSource 접속 정보. Hikari 세부 설정은 {@code spring.datasource.hikari.*} 가
 * MainDataSourceConfig 의 빈 메서드에서 직접 바인딩된다.
 *
 * <p>검증은 여기(빈 바인딩 시점)가 아니라 {@link MainDataSourceConfig#mainDataSource} 에서
 * 한다. 이 값들은 비어 있어도 정상일 수 있고 — Testcontainers·Compose 가 주는 접속 정보가 대신
 * 채운다 — 최종적으로 무엇이 쓰였는지는 그 자리에서만 알 수 있다. 비밀번호는 검증하지 않는다:
 * 빈 비밀번호 계정이 로컬에서 유효한 설정이라 기동을 막을 근거가 없다.
 */
@ConfigurationProperties("spring.datasource")
public record MainDbProperties(String driverClassName, String url, String username, String password) {
}
