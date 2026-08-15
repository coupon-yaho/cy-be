// 검증 어댑터들이 공유하는 컬럼 변환입니다. null 을 그대로 통과시키는 것이 전부입니다.
package com.kafkick.storage.verification;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.function.Function;

/**
 * 검증 스키마에는 nullable 컬럼이 많습니다 — 아직 끝나지 않은 실행의 판정,
 * 발급 이전 이력의 from_status, 취소되지 않은 사용의 canceled_at.
 * 어댑터마다 null 검사를 다시 쓰면 한 군데서 빠뜨렸을 때 조용히 틀립니다.
 */
final class ColumnValues {

    private ColumnValues() {
    }

    static Timestamp toTimestamp(LocalDateTime value) {
        return value == null ? null : Timestamp.valueOf(value);
    }

    static LocalDateTime toLocalDateTime(Timestamp value) {
        return value == null ? null : value.toLocalDateTime();
    }

    static String toName(Enum<?> value) {
        return value == null ? null : value.name();
    }

    static <E> E toEnum(String value, Function<String, E> parser) {
        return value == null ? null : parser.apply(value);
    }
}
