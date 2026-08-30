// 인덱스 선두 컬럼 비교를 DB 없이 잽니다.
package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>DDL 로는 못 잰다.</b> MySQL 의 {@code CREATE/DROP INDEX} 는 <b>암묵 커밋</b>을 일으켜
 * 같은 클래스의 다른 테스트들의 트랜잭션 격리를 깨뜨린다 — 실제로 넣어 봤다가 여섯 개가
 * 연달아 빨개졌다. 비교는 순수 함수이므로 여기서 잰다.
 *
 * <p>지키려는 성질은 <b>선두 컬럼</b>이다. {@code V2026082513} 헤더의 실측이 근거다 —
 * {@code (STATUS, END_TIME)} 은 {@code type=range rows=2,016} 인데
 * {@code (JOB_INSTANCE_ID, STATUS, END_TIME)} 은 {@code type=index rows=25,950} 이다.
 */
class IndexPrefixMatchTest {

    private static final String REQUIRED =
            "BATCH_JOB_EXECUTION.IX_JOB_EXEC_STATUS_END(STATUS,END_TIME)";

    @Test
    @DisplayName("정확히 같으면 있다")
    void acceptsAnExactMatch() {
        assertThat(VerificationRuleJdbcAdapter.satisfies(REQUIRED, REQUIRED)).isTrue();
    }

    @Test
    @DisplayName("선두가 맞으면 뒤에 더 붙어도 있다 — 그 질의를 그대로 태운다")
    void acceptsAnIndexThatExtendsTheRequiredPrefix() {
        assertThat(VerificationRuleJdbcAdapter.satisfies(
                "BATCH_JOB_EXECUTION.IX_JOB_EXEC_STATUS_END(STATUS,END_TIME,JOB_INSTANCE_ID)",
                REQUIRED))
                .as("완전 일치로 재면 멀쩡한 인덱스를 없다고 판정해 기동을 막는다 — "
                        + "없는 것을 막는 것보다 있는 것을 없다고 하는 편이 비싸다")
                .isTrue();
    }

    @Test
    @DisplayName("선두가 다르면 없다 — 그때는 질의가 인덱스를 못 탄다")
    void rejectsADifferentLeadingColumn() {
        assertThat(VerificationRuleJdbcAdapter.satisfies(
                "BATCH_JOB_EXECUTION.IX_JOB_EXEC_STATUS_END(JOB_INSTANCE_ID,STATUS,END_TIME)",
                REQUIRED))
                .isFalse();
    }

    @Test
    @DisplayName("컬럼이 모자라면 없다")
    void rejectsAShorterIndex() {
        assertThat(VerificationRuleJdbcAdapter.satisfies(
                "BATCH_JOB_EXECUTION.IX_JOB_EXEC_STATUS_END(STATUS)", REQUIRED))
                .isFalse();
    }

    @Test
    @DisplayName("이름이 다르면 컬럼이 같아도 없다")
    void rejectsADifferentName() {
        assertThat(VerificationRuleJdbcAdapter.satisfies(
                "BATCH_JOB_EXECUTION.SOME_OTHER_INDEX(STATUS,END_TIME)", REQUIRED))
                .isFalse();
    }

    /** MySQL 이 돌려주는 대소문자가 환경에 따라 갈린다 — 이름·컬럼 둘 다 무시해야 한다. */
    @Test
    @DisplayName("대소문자를 무시한다")
    void ignoresCase() {
        assertThat(VerificationRuleJdbcAdapter.satisfies(
                "batch_job_execution.ix_job_exec_status_end(status,end_time)", REQUIRED))
                .isTrue();
    }
}
