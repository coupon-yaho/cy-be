// 검증 어댑터들이 공유하는 컬럼 변환입니다. null 을 그대로 통과시키는 것이 전부입니다.
package com.kafkick.storage.db.verification;

import java.util.function.Function;

/**
 * 검증 스키마에는 nullable 컬럼이 많습니다 — 아직 끝나지 않은 실행의 판정,
 * 발급 이전 이력의 from_status, 취소되지 않은 사용의 canceled_at.
 * 어댑터마다 null 검사를 다시 쓰면 한 군데서 빠뜨렸을 때 조용히 틀립니다.
 *
 * <p><b>시각 변환 헬퍼는 없습니다.</b> {@code java.sql.Timestamp} 를 거치면 JVM 기본 타임존으로
 * 벽시계를 절대시각으로 바꾸고, 드라이버가 그것을 세션 타임존(UTC)으로 다시 렌더링합니다.
 * KST 기기에서 {@code asOf=14:00} 이 서버에서는 {@code 05:00} 으로 비교됩니다.
 * {@code datetime} 컬럼에는 타임존이 없으므로 {@link java.time.LocalDateTime} 을 그대로
 * 바인딩하고 {@code rs.getObject(name, LocalDateTime.class)} 로 읽습니다.
 */
final class ColumnValues {

    private ColumnValues() {
    }

    static String toName(Enum<?> value) {
        return value == null ? null : value.name();
    }

    static <E> E toEnum(String value, Function<String, E> parser) {
        return value == null ? null : parser.apply(value);
    }
}
