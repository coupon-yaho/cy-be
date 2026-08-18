// 중첩된 JDBC 예외에서 MySQL 오류 코드와 제약 이름을 일관되게 검사합니다.
package com.kafkick.storage.db.support;

import java.sql.SQLException;

public final class SqlErrorInspector {

    private SqlErrorInspector() {
    }

    public static boolean hasErrorCode(
            Throwable throwable,
            int errorCode
    ) {
        return hasErrorCode(throwable, errorCode, null);
    }

    public static boolean hasErrorCode(
            Throwable throwable,
            int errorCode,
            String messageKeyword
    ) {
        Throwable cause = throwable;
        while (cause != null) {
            if (cause instanceof SQLException sqlException
                    && matches(sqlException, errorCode, messageKeyword)) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static boolean matches(
            SQLException exception,
            int errorCode,
            String messageKeyword
    ) {
        return exception.getErrorCode() == errorCode
                && containsKeyword(exception.getMessage(), messageKeyword);
    }

    private static boolean containsKeyword(
            String message,
            String messageKeyword
    ) {
        return messageKeyword == null
                || (message != null && message.contains(messageKeyword));
    }
}
