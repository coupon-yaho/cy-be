// 검증 어댑터들이 공유하는 SHA-256 계산입니다.
package com.kafkick.storage.db.verification;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * <b>두 어댑터가 각자 구현하면 예외 메시지까지 두 벌이 된다.</b> 실제로 그랬고,
 * 해시 방식이나 실패 문구를 바꿀 때 한쪽만 고치면 검출 checksum 과 데이터셋 지문이 갈린다.
 * 두 값 모두 계약이 정한 SHA-256 이므로 계산 자리는 하나여야 한다.
 */
final class DigestValues {

    private DigestValues() {
    }

    /** SHA-256 은 JDK 가 항상 제공한다. 없으면 런타임이 깨진 것이라 복구할 방법이 없다. */
    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 을 쓸 수 없습니다.", e);
        }
    }

    /** 계약이 소문자 hex 를 쓴다. {@code HexFormat.of()} 의 기본값이 그것이다. */
    static String hex(byte[] digest) {
        return HexFormat.of().formatHex(digest);
    }

    static String sha256Hex(String material) {
        return hex(sha256().digest(material.getBytes(StandardCharsets.UTF_8)));
    }
}
