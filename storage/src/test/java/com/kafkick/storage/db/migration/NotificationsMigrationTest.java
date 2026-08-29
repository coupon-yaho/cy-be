package com.kafkick.storage.db.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

class NotificationsMigrationTest {

    private static MySQLContainer mysql;

    @BeforeAll
    static void migrate() {
        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                .withDatabaseName("app")
                .withCommand("--sql-mode=STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
                        + "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION");
        mysql.start();
        Flyway.configure()
                .dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    @AfterAll
    static void stop() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @AfterEach
    void clear() throws SQLException {
        execute("DELETE FROM notification_outbox");
        execute("DELETE FROM notification_resend_audits");
        execute("DELETE FROM notification_attempts");
        execute("DELETE FROM notifications");
    }

    @Test
    void migrationCreatesFourTablesAndOutboxChecks() throws SQLException {
        assertThat(query("SELECT COUNT(*) FROM information_schema.tables"
                + " WHERE table_schema=DATABASE() AND table_name IN"
                + " ('notifications','notification_attempts','notification_resend_audits',"
                + " 'notification_outbox')"))
                .isEqualTo(4);
        assertThat(query("SELECT COUNT(*) FROM information_schema.check_constraints"
                + " WHERE constraint_schema=DATABASE() AND constraint_name IN"
                + " ('ck_notifications_status','ck_notifications_sent_at',"
                + " 'ck_notifications_failure_reason','ck_resend_audits_reject')"))
                .isEqualTo(4);
    }

    @Test
    void uniqueKeysRejectDuplicateNotificationAndAttemptSequence() throws SQLException {
        insertNotification(1, "PENDING", "NULL", "NULL");
        assertThatThrownBy(() -> execute("INSERT INTO notifications"
                + " (id, coupon_id, member_id, issuance_id, status, recipient_contact, message_body,"
                + " created_at, updated_at) VALUES (101, 1, 1, 1, 'PENDING', 'contact', 'message', NOW(6), NOW(6) )"))
                .hasMessageContaining("uk_notifications_issuance_channel");

        insertAttempt(1, 1, "SUCCESS", "NULL");
        assertThatThrownBy(() -> insertAttempt(1, 1, "SUCCESS", "NULL"))
                .hasMessageContaining("uk_attempts_notification_seq");
    }

    @Test
    void checksRejectInconsistentStates() throws SQLException {
        assertThatThrownBy(() -> insertNotification(1, "SENT", "NULL", "NULL"))
                .hasMessageContaining("ck_notifications_sent_at");
        assertThatThrownBy(() -> execute("INSERT INTO notifications"
                + " (id, coupon_id, member_id, issuance_id, status, failed_at, recipient_contact,"
                + " message_body, created_at, updated_at)"
                + " VALUES (2, 2, 2, 2, 'FAILED', NOW(6), 'contact', 'message', NOW(6), NOW(6))"))
                .hasMessageContaining("ck_notifications_failure_reason");
        assertThatThrownBy(() -> execute("INSERT INTO notifications"
                + " (id, coupon_id, member_id, issuance_id, status, last_failure_reason,"
                + " recipient_contact, message_body, created_at, updated_at)"
                + " VALUES (3, 3, 3, 3, 'FAILED', 'SEND_TIMEOUT', 'contact', 'message', NOW(6), NOW(6))"))
                .hasMessageContaining("ck_notifications_failed_at");
        insertNotification(1, "PENDING", "NULL", "NULL");
        assertThatThrownBy(() -> insertAttempt(1, 1, "FAILED", "NULL"))
                .hasMessageContaining("ck_attempts_failure_reason");
        assertThatThrownBy(() -> execute("INSERT INTO notification_resend_audits"
                + " (notification_id, attempt_seq, requested_by, requested_at, accepted, reject_code, created_at)"
                + " VALUES (1, 1, 2, NOW(6), TRUE, 'ADMIN-006', NOW(6))"))
                .hasMessageContaining("ck_resend_audits_reject");
    }

    @Test
    void resendCountCannotExceedThree() {
        assertThatThrownBy(() -> execute("INSERT INTO notifications"
                + " (coupon_id, member_id, issuance_id, status, attempt_count, resend_count,"
                + " recipient_contact, message_body, created_at, updated_at)"
                + " VALUES (1, 1, 1, 'PENDING', 0, 4, 'contact', 'message', NOW(6), NOW(6))"))
                .hasMessageContaining("ck_notifications_resend_count");
    }

    @Test
    void rejectedAuditAllowsNullSequenceAndOutboxRejectsDuplicates() throws SQLException {
        insertNotification(1, "PENDING", "NULL", "NULL");
        execute("INSERT INTO notification_resend_audits"
                + " (notification_id, attempt_seq, requested_by, requested_at, accepted, reject_code, created_at)"
                + " VALUES (1, NULL, 2, NOW(6), FALSE, 'ADMIN-005', NOW(6))");
        execute("INSERT INTO notification_outbox"
                + " (notification_id, attempt_seq, `trigger`, status, next_attempt_at, created_at)"
                + " VALUES (1, 1, 'MANUAL', 'PENDING', NOW(6), NOW(6))");
        assertThatThrownBy(() -> execute("INSERT INTO notification_outbox"
                + " (notification_id, attempt_seq, `trigger`, status, next_attempt_at, created_at)"
                + " VALUES (1, 1, 'MANUAL', 'PENDING', NOW(6), NOW(6))"))
                .hasMessageContaining("uk_notification_outbox_attempt");
    }

    @Test
    void enumChecksRejectUnknownValuesAndAutoOutboxTrigger() throws SQLException {
        insertNotification(1, "PENDING", "NULL", "NULL");
        assertThatThrownBy(() -> execute("INSERT INTO notification_outbox"
                + " (notification_id, attempt_seq, `trigger`, status, next_attempt_at, created_at)"
                + " VALUES (1, 1, 'AUTO', 'PENDING', NOW(6), NOW(6))"))
                .hasMessageContaining("ck_notification_outbox_trigger");
        assertThatThrownBy(() -> execute("INSERT INTO notification_resend_audits"
                + " (notification_id, requested_by, requested_at, accepted, reject_code, created_at)"
                + " VALUES (1, 2, NOW(6), FALSE, 'ADMIN-999', NOW(6))"))
                .hasMessageContaining("ck_resend_audits_reject_code");
    }

    @Test
    void outboxClaimIndexesMatchPendingRecoveryAndTokenQueries() throws SQLException {
        assertThat(query("SELECT COUNT(*) FROM information_schema.statistics"
                + " WHERE table_schema=DATABASE() AND table_name='notification_outbox'"
                + " AND index_name IN ('ix_notification_outbox_pending',"
                + " 'ix_notification_outbox_expired','uk_notification_outbox_claim_token')"))
                .isEqualTo(7);
        insertNotification(1, "PENDING", "NULL", "NULL");
        execute("INSERT INTO notification_outbox"
                + " (notification_id,attempt_seq,`trigger`,status,next_attempt_at,created_at)"
                + " VALUES (1,1,'INITIAL','PENDING',NOW(6),NOW(6))");
        execute("INSERT INTO notification_outbox"
                + " (notification_id,attempt_seq,`trigger`,status,failure_count,next_attempt_at,"
                + " claimed_at,claim_token,created_at) VALUES"
                + " (1,2,'INITIAL','IN_PROGRESS',0,NOW(6),"
                + " TIMESTAMPADD(SECOND,-10,NOW(6)),'00000000-0000-4000-8000-000000000091',NOW(6))");

        assertThat(queryString("EXPLAIN SELECT id FROM notification_outbox"
                + " WHERE status='PENDING' AND next_attempt_at<=CURRENT_TIMESTAMP(6)"
                + " ORDER BY next_attempt_at,id LIMIT 1", "key"))
                .isEqualTo("ix_notification_outbox_pending");
        assertThat(queryString("EXPLAIN SELECT id,failure_count FROM notification_outbox"
                + " WHERE status='IN_PROGRESS'"
                + " AND claimed_at<TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6))"
                + " ORDER BY claimed_at,id LIMIT 1", "key"))
                .isEqualTo("ix_notification_outbox_expired");
        assertThat(queryString("EXPLAIN SELECT id FROM notification_outbox"
                + " WHERE claim_token='00000000-0000-4000-8000-000000000091'", "key"))
                .isEqualTo("uk_notification_outbox_claim_token");
    }

    private static void insertNotification(
            long issuanceId,
            String status,
            String failureReason,
            String sentAt
    ) throws SQLException {
        execute("INSERT INTO notifications"
                + " (id, coupon_id, member_id, issuance_id, status, recipient_contact, message_body,"
                + " created_at, updated_at, sent_at, last_failure_reason) VALUES"
                + " (" + issuanceId + ", " + issuanceId + ", " + issuanceId + ", " + issuanceId
                + ", '" + status + "', 'contact', 'message',"
                + " NOW(6), NOW(6), " + sentAt + ", " + failureReason + ")");
    }

    private static void insertAttempt(
            long notificationId,
            int attemptSeq,
            String result,
            String failureReason
    ) throws SQLException {
        execute("INSERT INTO notification_attempts"
                + " (notification_id, attempt_seq, `trigger`, result, failure_reason,"
                + " started_at, finished_at, created_at) VALUES"
                + " (" + notificationId + ", " + attemptSeq + ", 'INITIAL', '" + result + "', "
                + failureReason + ", NOW(6), NOW(6), NOW(6))");
    }

    private static void execute(String sql) throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private static int query(String sql) throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getInt(1);
        }
    }

    private static String queryString(String sql, String column) throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getString(column);
        }
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }
}
