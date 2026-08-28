package com.kafkick.api.admin.runtimeconfig.dto;

import java.time.Instant;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;

/**
 * RuntimeConfig Snapshot을 관리자 조회 API에 노출하는 HTTP 계약입니다.
 *
 * <p>Snapshot의 {@code status()}를 HTTP 필드명 {@code sourceStatus}로, 문자열
 * {@code updatedBy()}를 같은 이름의 응답 필드로 전달합니다.</p>
 *
 * @param revision 현재 설정 revision
 * @param engineVersion 현재 발급 엔진 버전
 * @param releaseStage 현재 릴리스 단계
 * @param queueMode 현재 대기열 진입 정책
 * @param updatedAt 설정이 마지막으로 변경된 시각
 * @param updatedBy 설정 변경 주체의 문자열 식별자
 * @param sourceStatus RuntimeConfig 원천의 canonical 관측 상태
 */
public record RuntimeConfigResponse(long revision, EngineVersion engineVersion, ReleaseStage releaseStage,
                                    QueueMode queueMode, Instant updatedAt, String updatedBy,
                                    SourceStatus sourceStatus) {

    /** Core Snapshot을 관리자 HTTP 응답 계약으로 변환합니다. */
    public static RuntimeConfigResponse from(RuntimeConfigSnapshot snapshot) {
        return new RuntimeConfigResponse(
                snapshot.revision(),
                snapshot.engineVersion(),
                snapshot.releaseStage(),
                snapshot.queueMode(),
                snapshot.updatedAt(),
                snapshot.updatedBy(),
                snapshot.status());
    }
}
