package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>선점이 도는 종류 목록이 스키마가 허용하는 값과 정확히 같다.</b>
 *
 * <p>선점은 {@link AttemptTrigger#outboxKinds()} 를 돌며 종류마다 몫을 뗀다. 그 목록에
 * 없는 값이 컬럼에 들어가면 <b>어느 회차에도 안 집혀 영원히 남는다</b> — 실패도 알림도
 * 없이 그 명령만 조용히 사라진다. 반대로 목록에만 있고 컬럼에 못 들어가는 값은
 * <b>영원히 0 인 시계열</b>을 만들어 대시보드를 오독하게 한다.
 *
 * <h2>제약 문구가 아니라 DB 가 받는지를 본다</h2>
 *
 * <p>{@code information_schema} 의 {@code CHECK_CLAUSE} 를 정규식으로 읽는 방법도
 * 있지만, 그것은 <b>문구를 읽는 것이지 동작을 보는 것이 아니다</b> — 콜레이션이나
 * 트리거처럼 문구 밖에서 값을 막는 것이 생기면 그대로 통과한다. 그래서 여기서는
 * <b>실제로 넣어 본다.</b>
 */
@RepositoryTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OutboxKindCoverageTest {

    /** 이 클래스가 쓰는 구간. 형제와 안 겹치게 좁게 잡고 그 구간만 지운다. */
    private static final long ID_BASE = 7_200_000L;
    private static final long ID_MAX = 7_209_999L;

    /** 값을 막는 제약. 다른 이유로 거절된 것을 "안 받는다" 로 읽지 않기 위해 이름으로 가른다. */
    private static final String TRIGGER_CONSTRAINT = "ck_notification_outbox_trigger";

    @Autowired JdbcTemplate jdbcTemplate;

    @BeforeEach
    @AfterEach
    void clean() {
        jdbcTemplate.update(
                "DELETE FROM notification_outbox WHERE notification_id BETWEEN ? AND ?",
                ID_BASE, ID_MAX);
    }

    @Test
    @DisplayName("DB 가 받아 주는 trigger 값과 선점이 도는 종류가 같다")
    void everyStorableTriggerIsClaimed() {
        for (AttemptTrigger candidate : AttemptTrigger.values()) {
            boolean storable = insertAccepted(candidate.name(), ID_BASE + candidate.ordinal());
            assertThat(storable)
                    .as("`%s` 는 DB 가 %s, 선점은 %s — 갈리면 그 종류가 조용히 남거나"
                            + " 영원히 0 인 시계열이 됩니다",
                            candidate, storable ? "받는데" : "안 받는데",
                            AttemptTrigger.outboxKinds().contains(candidate) ? "돕니다" : "안 돕니다")
                    .isEqualTo(AttemptTrigger.outboxKinds().contains(candidate));
        }
    }

    /**
     * <b>반대 방향</b> — 제약이 enum 밖의 값을 들이지 않는가.
     *
     * <p>제약에 {@code 'SYSTEM'} 같은 값이 늘면 그 행은 선점이 도는 목록에 없어
     * <b>어느 회차에도 안 집히거나</b>, 마지막 조회에서
     * {@code AttemptTrigger.valueOf} 가 터진다. 넣어 보기로는 절대 안 보인다.
     */
    @Test
    @DisplayName("제약이 허용하는 값 목록이 선점이 도는 종류를 벗어나지 않는다")
    void theConstraintAdmitsNothingBeyondTheClaimedKinds() {
        String clause = jdbcTemplate.queryForObject("""
                SELECT check_clause FROM information_schema.check_constraints
                 WHERE constraint_schema = DATABASE() AND constraint_name = ?
                """, String.class, TRIGGER_CONSTRAINT);

        // MySQL 은 절을 이스케이프해서 보관한다(실측: `trigger` ... in (_utf8mb4\\'INITIAL\\',...)).
        // 역슬래시를 먼저 걷어내고 홑따옴표 리터럴만 읽는다.
        Set<String> allowed = new LinkedHashSet<>();
        Matcher literals = Pattern.compile("'([^']*)'")
                .matcher(String.valueOf(clause).replace("\\", ""));
        while (literals.find()) {
            allowed.add(literals.group(1));
        }

        assertThat(allowed)
                .as("제약 문구=%s — 읽어낸 값이 없으면 이 테스트가 아무것도 안 봅니다", clause)
                .isNotEmpty();
        assertThat(allowed)
                .as("제약에만 있고 선점이 안 도는 값은 조용히 남습니다")
                .containsExactlyInAnyOrderElementsOf(
                        AttemptTrigger.outboxKinds().stream().map(Enum::name).toList());
    }

    /**
     * 대소문자는 <b>다른 값</b>이다. 컬럼 콜레이션이 {@code utf8mb4_0900_as_cs} 라
     * {@code 'initial'} 은 거절된다 — 미터 태그가 소문자를 쓰므로 헷갈릴 여지를 닫는다.
     */
    @Test
    @DisplayName("소문자 trigger 는 안 들어간다")
    void lowercaseTriggerIsRejected() {
        assertThat(insertAccepted("initial", ID_BASE + 9_000)).isFalse();
    }

    /**
     * 넣어 보고 받아들여졌는지 돌려준다.
     *
     * <p><b>{@code DataIntegrityViolationException} 이 안 온다 — 재 보고 알았다.</b>
     * MySQL 의 CHECK 위반은 {@code SQLSTATE HY000}(오류 코드 3819)인데, 이 저장소는
     * 사용자 {@code sql-error-codes.xml} 이 없어 번역기가
     * {@code SQLExceptionSubclassTranslator} 다(실측 — 런타임에 클래스를 찍어 봤다).
     * Connector/J 가 이 예외를 {@code SQLIntegrityConstraintViolationException} 으로
     * 감싸지 않고, 폴백인 {@code SQLStateSQLExceptionTranslator} 도 {@code HY} 를
     * 모르므로 {@code UncategorizedSQLException} 이 된다.
     *
     * <p>그래서 {@link DataAccessException} 으로 받되 <b>그 제약이 맞는지 확인하고</b>
     * 아니면 다시 던진다 — 넓게 삼키면 배선이 깨져도 "DB 가 안 받는다" 로 읽힌다.
     */
    private boolean insertAccepted(String trigger, long notificationId) {
        try {
            jdbcTemplate.update("""
                    INSERT INTO notification_outbox
                           (notification_id, attempt_seq, `trigger`, status, failure_count,
                            next_attempt_at, created_at)
                    VALUES (?, 1, ?, 'PENDING', 0, NOW(6), NOW(6))
                    """, notificationId, trigger);
            return true;
        } catch (DataAccessException rejected) {
            String message = String.valueOf(rejected.getMessage());
            if (!message.contains(TRIGGER_CONSTRAINT)) {
                throw rejected;
            }
            return false;
        }
    }
}
