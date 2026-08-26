package com.kafkick.core.coupon.service.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Pattern;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.ErrorCode;

/**
 * 멱등키 형식 검증과 요청 정규화 해시를 두 실행 경로가 함께 씁니다.
 *
 * <p>2단계 쓰기를 쓰는 사용·취소({@link IdempotencyExecutionService})와 한 번만 쓰는 발급이
 * 같은 규칙으로 키를 거르고 같은 방식으로 요청 동등성을 판단해야 하므로 한 곳에 둡니다.
 */
public final class IdempotencyKeys {

    private static final Pattern UUID_V4_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-"
                    + "[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    );

    private IdempotencyKeys() {
    }

    /**
     * UUID v4 형식이 아닌 멱등키를 업무 오류로 거릅니다.
     *
     * @param idempotencyKey 클라이언트가 보낸 멱등키
     * @param invalidRequestErrorCode 형식 위반에 사용할 업무 오류 코드
     * @throws BusinessException 형식을 충족하지 못한 경우
     */
    public static void validate(
            String idempotencyKey,
            ErrorCode invalidRequestErrorCode
    ) {
        if (idempotencyKey == null
                || !UUID_V4_PATTERN.matcher(idempotencyKey).matches()) {
            throw new BusinessException(
                    invalidRequestErrorCode,
                    "Idempotency-Key must be UUID v4"
            );
        }
    }

    /**
     * 정규화 요청 문자열의 SHA-256 16진 해시를 만듭니다.
     *
     * @param canonicalRequest 요청 동등성을 판단할 정규화 문자열
     * @return 64자리 16진 해시
     */
    public static String hash(String canonicalRequest) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(
                    canonicalRequest.getBytes(StandardCharsets.UTF_8)
            ));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(
                    "SHA-256 알고리즘을 사용할 수 없습니다.",
                    exception
            );
        }
    }
}
