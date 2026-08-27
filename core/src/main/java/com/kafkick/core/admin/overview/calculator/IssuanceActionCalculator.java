package com.kafkick.core.admin.overview.calculator;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/** O1 발급 흐름 관측에서 발급 중단 조치 후보만 추출하는 순수 계산기입니다. */
@Component
public class IssuanceActionCalculator {

    /** 상태 없는 O1 조치 후보 계산기를 생성합니다. */
    public IssuanceActionCalculator() { }

    /**
     * 최신 정상 O1 관측의 발급 중단만 조치 후보로 변환합니다.
     *
     * <p>오래된·준비 중·미수집 관측과 발급 감소는 운영 조치로 승격하지 않습니다. 이 계산기는
     * 화면 표시용 캠페인 이름·오픈 시각을 모르며 Service 조립 경계가 같은 couponId의 정보를
     * 보강합니다.</p>
     *
     * @param issuanceFlows couponId별 계산 완료 O1 관측값
     * @return couponId별로 최대 한 건인 기술 중립 발급 중단 조치 후보
     * @throws NullPointerException Map 또는 Map의 key·value가 null인 경우
     */
    public List<AdminOverviewSnapshot.OperationActionItem> calculate(
            Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> issuanceFlows
    ) {
        Objects.requireNonNull(issuanceFlows, "issuanceFlows");
        return issuanceFlows.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .filter(entry -> isStoppedValidObservation(entry.getValue()))
                .map(entry -> stoppedAction(entry.getKey(), entry.getValue()))
                .toList();
    }

    /** 최신 정상 원천이면서 O1 상태가 STOPPED일 때만 실제 조치 후보로 승격합니다. */
    private static boolean isStoppedValidObservation(
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> observation
    ) {
        Objects.requireNonNull(observation, "issuanceFlows에는 null을 포함할 수 없습니다.");
        // 최신 정상 관측에서 확인된 중단만 운영자 조치로 승격합니다.
        return observation.status() == SourceStatus.VALID
                && observation.value().state() == AdminOverviewSnapshot.IssuanceFlowState.STOPPED;
    }

    /** O1 평가 구간 종료와 연속 시간에서 최초 감지 시각을 역산해 기술 중립 후보를 만듭니다. */
    private static AdminOverviewSnapshot.OperationActionItem stoppedAction(
            Long couponId,
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> observation
    ) {
        Objects.requireNonNull(couponId, "issuanceFlows에는 null couponId를 포함할 수 없습니다.");
        AdminOverviewSnapshot.IssuanceFlow flow = observation.value();
        Instant detectedAt = flow.stateDuration() == null
                ? null
                : flow.windowEnd().minus(flow.stateDuration());
        return new AdminOverviewSnapshot.OperationActionItem(couponId, null, null, Severity.CRITICAL,
                AdminOverviewSnapshot.CustomerImpact.LIMITED, "쿠폰 발급이 중단되었습니다.", detectedAt,
                flow.stateDuration(), new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.ISSUANCE_STOPPED,
                        "발급 흐름 확인", AdminOverviewSnapshot.TargetScreen.ISSUANCE_INQUIRY));
    }
}
