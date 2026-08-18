// 멱등키 선점부터 쿠폰 사용·실적·이력·최초 응답 저장까지 한 트랜잭션으로 묶습니다.
package com.kafkick.api.coupon.adapter;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.api.coupon.dto.CouponUseRequest;
import com.kafkick.api.coupon.dto.CouponUseResponse;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.service.CouponUseCommand;
import com.kafkick.core.coupon.service.CouponUseResult;
import com.kafkick.core.coupon.service.CouponUseService;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

@Component
public class CouponUseTransactionalAdapter {

    private final CouponUseService couponUseService;
    private final IdempotencyRepository idempotencyRepository;
    private final TimeProvider timeProvider;
    private final ObjectMapper objectMapper;

    public CouponUseTransactionalAdapter(
            CouponUseService couponUseService,
            IdempotencyRepository idempotencyRepository,
            TimeProvider timeProvider,
            ObjectMapper objectMapper
    ) {
        this.couponUseService = couponUseService;
        this.idempotencyRepository = idempotencyRepository;
        this.timeProvider = timeProvider;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public CouponUseResponse use(
            Long issuanceId,
            Long memberId,
            String idempotencyKey,
            CouponUseRequest request
    ) {
        validateIdempotencyKey(idempotencyKey);
        String requestHash = hashRequest(
                issuanceId,
                memberId,
                request
        );
        boolean firstRequest = idempotencyRepository.tryStart(
                idempotencyKey,
                requestHash,
                timeProvider.instant()
        );
        if (!firstRequest) {
            return replay(idempotencyKey, requestHash);
        }

        CouponUseResult result = couponUseService.use(
                new CouponUseCommand(
                        issuanceId,
                        memberId,
                        request.orderId(),
                        request.orderAmount(),
                        idempotencyKey,
                        timeProvider.instant()
                )
        );
        CouponUseResponse response = CouponUseResponse.from(result);
        idempotencyRepository.complete(
                idempotencyKey,
                memberId,
                issuanceId,
                writeResponse(response)
        );
        return response;
    }

    private CouponUseResponse replay(
            String idempotencyKey,
            String requestHash
    ) {
        IdempotencyRecord record = idempotencyRepository
                .findByKey(idempotencyKey)
                .orElseThrow(() -> new BusinessException(
                        CouponUseErrorCode.CONFLICT_IN_PROGRESS,
                        "idempotencyKey=" + idempotencyKey
                ));
        if (!record.requestHash().equals(requestHash)) {
            throw new BusinessException(
                    CouponUseErrorCode.IDEMPOTENCY_KEY_REUSED,
                    "idempotencyKey=" + idempotencyKey
            );
        }
        if (record.status() != IdempotencyStatus.DONE
                || record.responseBody() == null) {
            throw new BusinessException(
                    CouponUseErrorCode.CONFLICT_IN_PROGRESS,
                    "idempotencyKey=" + idempotencyKey
            );
        }
        return readResponse(record.responseBody());
    }

    private static void validateIdempotencyKey(String idempotencyKey) {
        try {
            UUID uuid = UUID.fromString(idempotencyKey);
            if (uuid.version() != 4
                    || !uuid.toString().equalsIgnoreCase(idempotencyKey)) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw new BusinessException(
                    CouponUseErrorCode.INVALID_COUPON_USE_REQUEST,
                    "Idempotency-Key must be UUID v4"
            );
        }
    }

    private static String hashRequest(
            Long issuanceId,
            Long memberId,
            CouponUseRequest request
    ) {
        String canonical = "USE|issuanceId=" + issuanceId
                + "|memberId=" + memberId
                + "|orderId=" + request.orderId()
                + "|orderAmount=" + request.orderAmount();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(
                    digest.digest(canonical.getBytes(StandardCharsets.UTF_8))
            );
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(
                    "SHA-256 알고리즘을 사용할 수 없습니다.",
                    exception
            );
        }
    }

    private String writeResponse(CouponUseResponse response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JacksonException exception) {
            throw new BusinessException(
                    CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED,
                    "쿠폰 사용 응답 직렬화에 실패했습니다.",
                    exception
            );
        }
    }

    private CouponUseResponse readResponse(String responseBody) {
        try {
            return objectMapper.readValue(
                    responseBody,
                    CouponUseResponse.class
            );
        } catch (JacksonException exception) {
            throw new BusinessException(
                    CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED,
                    "저장된 쿠폰 사용 응답을 읽지 못했습니다.",
                    exception
            );
        }
    }
}
