package com.kafkick.storage.db.admin.issuancehistory;
public interface AdminIssuanceHistorySummaryProjection { Long getIssueCount(); Long getUseCount(); Long getCancelUseCount(); Long getCancelCount(); Long getExpireCount(); }
