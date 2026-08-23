// 만료 배치가 오염 스키마를 보고 있지 않은지 잡 시작 전에 확인합니다.
package com.kafkick.batch.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.stereotype.Component;

import com.kafkick.core.expiration.exception.ExpirationErrorCode;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.VerificationRuleRepository;

/**
 * <b>만료는 원본을 쓰는 유일한 배치다. 오염셋을 보게 띄우면 정답지가 무너진다.</b>
 *
 * <p>오염 유형 2({@code history 는 USED 인데 issuances.status 는 ISSUED})와
 * 7({@code status 는 ISSUED 인데 활성 usages 행이 남아 있음})은 <b>둘 다 {@code ISSUED}</b> 다.
 * 만료가 그 200건을 {@code EXPIRED} 로 넘기고 {@code EXPIRE} 이력까지 붙이면, 리플레이가
 * {@code USED → EXPIRED} 라는 전이표에 없는 조합을 만나 <b>{@code expected_findings} 에 없는
 * 검출</b>이 생긴다. {@code dataset_fingerprint} 도 함께 움직인다.
 *
 * <p>그 결과가 나쁜 것은 <b>모양 때문</b>이다. 누락 0 · 오탐 0 이 이 과제의 합격 조건인데,
 * 그것이 <i>"검증기가 틀렸다"</i> 로 보이는 형태로 깨진다. 실제 원인은 검증기가 아니라
 * <b>만료가 한 번 지나갔다</b> 는 것이고, 그 사실은 어디에도 안 적힌다.
 *
 * <p><b>회차 격리(CY-347)가 이 위험을 넓혔다.</b> 예전에는 첫 오염 회차에서 잡이 죽어
 * 그 뒤로는 아무것도 안 건드렸다. 지금은 막힌 회차만 빼고 <b>나머지 전부</b>를 넘긴다 —
 * 폭을 넓힌 변경이 그 폭을 막는 가드도 함께 진다.
 *
 * <p><b>{@code verifyJob} 의 {@code rejectRunningExpire} 로는 못 막는다.</b> 그것은
 * <i>검증이 도는 동안</i> 을 막지, 그 <b>전에</b> 만료가 한 번 지나간 것은 못 본다.
 *
 * <p><b>왜 검증 포트를 빌려 쓰나.</b> <i>"지금 어느 스키마를 보고 있나"</i> 를 판정하는 근거
 * ({@code uk_coupon_member} 의 존재)는 이미 {@link VerificationRuleRepository} 에 있고,
 * {@code verifyJob} 의 {@code rejectDatasetMismatch} 가 그것을 쓴다. 같은 사실을 두 곳이
 * 각자 판정하면 <b>둘이 어긋나는 날 어느 쪽이 맞는지 아무도 모른다.</b> 그래서 복사하지 않는다.
 *
 * <p><b>이 가드가 증명하는 것은 스키마의 "모양" 이지 데이터가 깨끗하다는 것이 아니다.</b>
 * CORRUPT 가 떼는 제약은 {@code uk_coupon_member}·{@code uk_coupon_code}·
 * {@code ck_stock_range} 셋이고, 그것을 어기는 오염 유형은 1·3·5·6 이다. 정작 여기서
 * 막으려는 <b>유형 2·7 은 어느 제약도 안 어겨서 CLEAN 스키마에도 심을 수 있다.</b>
 * 지금 시드가 항상 셋을 함께 떼므로 계약상 안전하지만, 제약을 안 어기는 오염 변종을
 * 만드는 티켓은 <b>근거를 하나 더 얹어야 한다</b>(예: {@code expected_findings} 매니페스트의
 * 존재). 지금 근거를 늘리지 않는 것은 {@code verifyJob} 의 {@code rejectDatasetMismatch} 와
 * 판정이 갈리면 이 가드가 피하려던 문제가 그대로 생기기 때문이다.
 *
 * <p><b>이것은 판정이 아니라 실패다.</b> <i>"데이터가 틀렸다"</i> 가 아니라 <i>"여기서 돌면
 * 안 되는 배치가 돌았다"</i> 이고, 고칠 곳은 데이터가 아니라 접속 설정이다. 설계 원칙이
 * 말하는 <i>"판정을 내지 못한 경우"</i> 에 해당한다.
 */
@Component
public class CleanSchemaGuard implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(CleanSchemaGuard.class);

    private final VerificationRuleRepository rules;

    public CleanSchemaGuard(VerificationRuleRepository rules) {
        this.rules = rules;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        if (!rules.hasCleanOnlyConstraints()) {
            throw new BusinessException(
                    ExpirationErrorCode.EXPIRE_ON_CORRUPT_SCHEMA,
                    "uk_coupon_member 가 없습니다 — 오염 스키마를 보고 있습니다. "
                            + "만료는 원본을 쓰는 유일한 배치라, 여기서 돌면 오염셋의 ISSUED "
                            + "발급건이 EXPIRED 로 넘어가 expected_findings 와 어긋납니다. "
                            + "접속 URL 이 가리키는 DB 를 확인하세요.");
        }
        log.info("만료 배치가 CLEAN 스키마를 보고 있습니다.");
    }
}
