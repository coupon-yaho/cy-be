package com.kafkick.core.coupon.service;

import java.util.Objects;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 발급 요청의 완료 여부와 정책 통과 여부를 <b>읽기 전용 트랜잭션 한 번</b>에 확인합니다.
 *
 * <p>이전에는 멱등 선점({@code tryStart})이 별도 쓰기 트랜잭션으로 먼저 커밋되고, 정책 사전검증이
 * 그 다음 읽기 트랜잭션에서 돌았습니다. 발급은 {@code uk_coupon_member}가 멱등 선점과 같은 배제를
 * 이미 제공하므로 선점을 먼저 보여줄 이유가 없어, 두 일을 이 한 트랜잭션으로 합쳤습니다.
 *
 * <p>정책 검증은 {@link CouponIssuePolicyValidator}가 그대로 맡습니다. 그쪽 경계가
 * {@code REQUIRED}라 이 트랜잭션에 합류하므로 경계는 하나로 유지됩니다.
 */
@Service
public class CouponIssuePreflightService {

    private final IdempotencyRepository idempotencyRepository;
    private final CouponIssuePolicyValidator policyValidator;

    public CouponIssuePreflightService(
            IdempotencyRepository idempotencyRepository,
            CouponIssuePolicyValidator policyValidator
    ) {
        this.idempotencyRepository = Objects.requireNonNull(
                idempotencyRepository
        );
        this.policyValidator = Objects.requireNonNull(policyValidator);
    }

    /**
     * 저장된 멱등 결과를 먼저 보고, 없으면 발급 정책을 검증합니다.
     *
     * @param command 발급 요청
     * @param requestHash 요청 정규화 해시
     * @return 완료된 응답 또는 진행 지시
     * @throws BusinessException 멱등키가 다른 요청에 쓰였거나 정책을 충족하지 못한 경우
     */
    @Transactional(readOnly = true)
    public CouponIssuePreflight inspect(
            CouponIssueCommand command,
            String requestHash
    ) {
        Optional<IdempotencyRecord> stored = idempotencyRepository.findByKey(
                command.idempotencyKey()
        );
        if (stored.isPresent()) {
            return inspectStored(stored.get(), requestHash);
        }
        policyValidator.validate(command);
        return CouponIssuePreflight.pending();
    }

    /**
     * 권위 트랜잭션이 롤백된 뒤 커밋된 완료 응답만 다시 읽습니다.
     *
     * <p>정책 재검증은 하지 않습니다 — 이 시점의 관심사는 "먼저 확정된 결과가 있는가" 하나입니다.
     *
     * @param idempotencyKey UUID v4 멱등 키
     * @param requestHash 요청 정규화 해시
     * @return 같은 요청의 완료 응답. 없으면 빈 값
     */
    @Transactional(readOnly = true)
    public Optional<String> findCompletedResponse(
            String idempotencyKey,
            String requestHash
    ) {
        Optional<IdempotencyRecord> stored = idempotencyRepository.findByKey(
                idempotencyKey
        );
        if (stored.isEmpty()) {
            return Optional.empty();
        }
        IdempotencyRecord record = stored.get();
        if (!record.requestHash().equals(requestHash)) {
            throw new BusinessException(
                    CouponUseErrorCode.IDEMPOTENCY_KEY_REUSED,
                    "idempotencyKey=" + record.key()
            );
        }
        if (record.status() != IdempotencyStatus.DONE) {
            return Optional.empty();
        }
        return Optional.of(record.responseBody());
    }

    /**
     * 같은 멱등키의 저장된 레코드를 요청 해시와 상태로 판정합니다.
     *
     * <p>{@code IN_PROGRESS}는 발급 경로가 더는 만들지 않지만, 사용·취소가 같은 테이블에 2단계로
     * 쓰므로 방어적으로 남겨 둡니다.
     */
    private static CouponIssuePreflight inspectStored(
            IdempotencyRecord record,
            String requestHash
    ) {
        if (!record.requestHash().equals(requestHash)) {
            throw new BusinessException(
                    CouponUseErrorCode.IDEMPOTENCY_KEY_REUSED,
                    "idempotencyKey=" + record.key()
            );
        }
        if (record.status() == IdempotencyStatus.DONE) {
            return CouponIssuePreflight.completed(record.responseBody());
        }
        throw new BusinessException(
                CouponUseErrorCode.CONFLICT_IN_PROGRESS,
                "idempotencyKey=" + record.key()
        );
    }
}
