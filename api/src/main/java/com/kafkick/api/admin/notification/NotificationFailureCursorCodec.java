package com.kafkick.api.admin.notification;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.springframework.stereotype.Component;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

@Component
public class NotificationFailureCursorCodec {
    private static final int MAX_CURSOR_LENGTH = 256;
    private static final String VERSION = "v1";
    private static final String INVALID_CURSOR_MESSAGE = "유효하지 않은 알림 cursor입니다.";

    public String encode(long notificationId) {
        if (notificationId <= 0) {
            throw new IllegalArgumentException("notificationId는 양수여야 합니다.");
        }
        String payload = VERSION + "|" + notificationId;
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }

    public long decode(String cursor) {
        if (cursor != null && cursor.length() > MAX_CURSOR_LENGTH) {
            throw invalidCursor(null);
        }
        try {
            if (cursor == null || cursor.isBlank() || cursor.indexOf('=') >= 0) {
                throw new IllegalArgumentException("cursor 형식 오류");
            }
            byte[] decoded = Base64.getUrlDecoder().decode(cursor);
            String canonical = Base64.getUrlEncoder().withoutPadding().encodeToString(decoded);
            if (!canonical.equals(cursor)) {
                throw new IllegalArgumentException("cursor canonical 형식 오류");
            }
            String[] segments = new String(decoded, StandardCharsets.UTF_8).split("\\|", -1);
            if (segments.length != 2 || !VERSION.equals(segments[0])) {
                throw new IllegalArgumentException("cursor payload 오류");
            }
            long notificationId = Long.parseLong(segments[1]);
            if (notificationId <= 0) {
                throw new IllegalArgumentException("cursor 값 범위 오류");
            }
            return notificationId;
        } catch (RuntimeException failure) {
            throw invalidCursor(failure);
        }
    }

    private static BusinessException invalidCursor(Throwable cause) {
        return cause == null
                ? new BusinessException(CommonErrorCode.INVALID_INPUT, INVALID_CURSOR_MESSAGE)
                : new BusinessException(CommonErrorCode.INVALID_INPUT, INVALID_CURSOR_MESSAGE, cause);
    }
}
