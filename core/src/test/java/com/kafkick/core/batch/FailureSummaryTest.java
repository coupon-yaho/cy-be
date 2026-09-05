package com.kafkick.core.batch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 실패 원문 요약.
 *
 * <p>이 클래스는 <b>정규식 둘이 전부</b>라 잘못 넓히면 조용히 틀린다 — 오탐해도 예외가 안
 * 나고, 화면에는 그럴듯한 문자열이 찍힌다. 그래서 넓히려던 모양이 왜 안 되는지까지 여기
 * 박아 둔다.
 */
class FailureSummaryTest {

    @Test
    @DisplayName("도메인 에러코드가 있으면 그것만 — 스택트레이스는 안 싣는다")
    void keepsDomainCode() {
        assertThat(FailureSummary.of("""
                com.kafkick.core.support.exception.BusinessException: VERIFICATION-003 \
                결정론 위반\tat com.kafkick.batch.job.VerifyJobConfig.finalizeRun"""))
                .isEqualTo("VERIFICATION-003");
        assertThat(FailureSummary.of("EXPIRATION-002 재고 불일치")).isEqualTo("EXPIRATION-002");
        assertThat(FailureSummary.of("COMMON-001 알 수 없는 요청")).isEqualTo("COMMON-001");
    }

    @Test
    @DisplayName("코드가 없으면 예외 클래스 이름만 — 메시지에는 SQL 조각이 섞인다")
    void keepsExceptionTypeOnly() {
        assertThat(FailureSummary.of(
                "java.sql.SQLSyntaxErrorException: Unknown column 'x' in 'field list'"))
                .as("컬럼명·테이블명이 응답으로 나가면 스키마를 밖에서 그릴 수 있다")
                .isEqualTo("SQLSyntaxErrorException");
        assertThat(FailureSummary.of("Caused by: java.lang.OutOfMemoryError: Java heap space"))
                .isEqualTo("OutOfMemoryError");
    }

    @Test
    @DisplayName("코드 접두사를 [A-Z]+ 로 넓히면 안 된다 — ISO-8859-1 이 ISO-885 로 잡힌다")
    void doesNotMatchArbitraryUppercaseDashDigits() {
        assertThat(FailureSummary.of("charset ISO-8859-1 미지원, VERIFICATION-007"))
                .as("넓힌 정규식은 앞의 ISO-885 를 잡고, find() 는 첫 매치를 쓰므로 "
                        + "뒤의 진짜 코드가 가려진다")
                .isEqualTo("VERIFICATION-007");
    }

    @Test
    @DisplayName("코드가 둘이면 원문에서 먼저 나온 것 — 열거 순서가 아니라 위치가 정한다")
    void takesLeftmostCodeInTheMessage() {
        // 실측: 대안(alternation)의 순서는 무관하고 입력에서 왼쪽에 있는 것이 이긴다.
        // 감싼 예외가 먼저 찍히므로 그것이 곧 바깥 원인이고, 요약에는 그쪽이 맞다.
        assertThat(FailureSummary.of("COMMON-004 처리 중: VERIFICATION-003 결정론 위반"))
                .isEqualTo("COMMON-004");
        assertThat(FailureSummary.of("VERIFICATION-003 결정론 위반; 원인 COMMON-004"))
                .isEqualTo("VERIFICATION-003");
    }

    @Test
    @DisplayName("못 알아본 것과 원인이 없는 것을 가른다 — 둘 다 null 이면 화면이 성공으로 읽는다")
    void separatesUnknownFromNotRecorded() {
        assertThat(FailureSummary.of(null)).isEqualTo(FailureSummary.NOT_RECORDED);
        assertThat(FailureSummary.of("   ")).isEqualTo(FailureSummary.NOT_RECORDED);
        assertThat(FailureSummary.of("회차 147 · 등급쌍 468 · ISSUE 이력 3000000건"))
                .as("verifyJob 이 성공했을 때 EXIT_MESSAGE 에 실제로 들어가는 문자열이다. "
                        + "실패한 실행에만 부르는 것은 BatchRunView 의 책임이고, 여기 오면 "
                        + "알아볼 수 없는 것이 맞다")
                .isEqualTo(FailureSummary.UNKNOWN);
    }

    @Test
    @DisplayName("무엇을 주든 null 을 안 낸다 — 부르는 쪽이 널가드를 또 두면 판정이 갈린다")
    void neverReturnsNull() {
        assertThat(FailureSummary.of("")).isNotNull();
        assertThat(FailureSummary.of("\n\t")).isNotNull();
        assertThat(FailureSummary.of("...")).isNotNull();
    }
}
