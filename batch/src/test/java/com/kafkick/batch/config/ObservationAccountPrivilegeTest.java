package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>관측 계정이 정말 SELECT 전용인지, 그리고 읽어야 할 것을 실제로 읽는지</b>를 본다.
 *
 * <p>이 계약은 지금까지 <b>코드 주석에만</b> 있었다. {@code .env.example} 은
 * {@code DB_OBS_USERNAME=<select-only-user>} 라는 플레이스홀더뿐이고 GRANT 산출물이 없었으며,
 * 테스트 픽스처는 컨테이너 슈퍼유저를 관측 자리에 꽂았다. 즉 <b>권한이 모자라도 프로덕션에서만
 * 500 이 났다.</b>
 *
 * <p><b>{@code BATCH_*} 를 특히 본다.</b> 이력 조회가 관측 풀로 읽는 그 아홉 테이블은 우리가
 * 만든 것이 아니라 Spring Batch 가 만든 것이다. GRANT 를 테이블 단위로 열거하는 방식이었다면
 * <b>여기가 가장 먼저 빠질 자리</b>다. 계정과 권한의 정본은
 * {@code infra/mysql/initdb/20-obs-account.sh} 다.
 */
@SpringBootTest(properties = "spring.flyway.enabled=true")
@Import(MySqlContainerConfig.class)
class ObservationAccountPrivilegeTest {

    @Autowired
    @Qualifier("obs")
    JdbcTemplate observationJdbcTemplate;

    @Autowired
    @Qualifier("obs")
    DataSource observationDataSource;

    @Test
    @DisplayName("관측 계정이 배치 메타 테이블을 읽을 수 있다")
    void canReadBatchMetadataTables() {
        assertThat(observationJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Integer.class)).isNotNull();
        assertThat(observationJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM BATCH_JOB_INSTANCE", Integer.class)).isNotNull();
    }

    @Test
    @DisplayName("관측 계정은 read-only 플래그를 꺼도 쓰지 못한다 — GRANT 가 막는다")
    void cannotWriteEvenWithoutReadOnlyFlag() {
        // Hikari 의 read-only 를 켠 채로 시도하면 드라이버가 클라이언트 쪽에서 먼저 거부한다
        // (TransientDataAccessResourceException: Connection is read-only). 그것은 세션 속성일
        // 뿐이라 누가 설정 한 줄을 지우면 사라진다 — 그 상태에서도 막히는지가 이 테스트의 질문이다.
        assertThatThrownBy(() -> {
            try (Connection connection = observationDataSource.getConnection()) {
                connection.setReadOnly(false);
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate(
                            "INSERT INTO BATCH_JOB_INSTANCE"
                                    + " (JOB_INSTANCE_ID, VERSION, JOB_NAME, JOB_KEY)"
                                    + " VALUES (999, 0, 'x', 'x')");
                }
            }
        })
                .isInstanceOf(SQLException.class)
                // MySQL 1142 = 명령에 대한 권한 없음. 메시지가 아니라 코드로 본다.
                .satisfies(thrown -> assertThat(((SQLException) thrown).getErrorCode())
                        .as("GRANT 가 아니라 다른 이유로 막혔다면 이 계약은 검증되지 않는다")
                        .isEqualTo(1142));
    }
}
