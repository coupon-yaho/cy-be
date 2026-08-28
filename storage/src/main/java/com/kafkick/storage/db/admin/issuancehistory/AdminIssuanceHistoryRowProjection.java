package com.kafkick.storage.db.admin.issuancehistory;
import java.time.Instant;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
public interface AdminIssuanceHistoryRowProjection { Long getHistoryId(); Long getIssuanceId(); String getIssuanceCode(); Long getCouponId(); IssuanceStatus getFromStatus(); IssuanceStatus getToStatus(); IssuanceEventType getEventType(); String getReason(); String getRequestId(); Instant getOccurredAt(); }
