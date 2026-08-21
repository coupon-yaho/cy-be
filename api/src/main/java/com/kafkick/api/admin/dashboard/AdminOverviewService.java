package com.kafkick.api.admin.dashboard;

import java.time.Instant;
import java.util.Objects;

import org.springframework.stereotype.Service;

import com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataFactory;
import com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset;
import com.kafkick.core.admin.overview.AdminOverviewResult;
import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator.CampaignCalculation;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator.ActionCalculation;
import com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/**
 * 관리자 첫 화면에 필요한 운영현황 조회와 결과 조립 흐름을 담당합니다.
 *
 * <p>현재 캠페인 Repository가 준비되기 전까지 Mock Factory에서 원천을 조회하고, 원천별 결과를
 * Calculator에 전달해 {@link AdminOverviewResult}를 생성합니다. 계산식과 정책 판정은 전용
 * Calculator가 담당하며 Service는 조회 순서와 결과 조립만 조정합니다.</p>
 *
 * <p>캠페인 Repository가 병합되면 Mock Factory 호출을 실제 조회와 캠페인 계산 입력 변환으로
 * 교체합니다. Calculator와 Snapshot 조립은 유지하며, 아직 연결되지 않은 관측 영역은 수치를
 * 추정하지 않고 {@link SourceStatus#UNAVAILABLE}로 제공합니다.</p>
 */
@Service
public class AdminOverviewService {

    private final TimeProvider timeProvider;
    private final AdminOverviewMockDataFactory mockDataFactory;
    private final CampaignOverviewCalculator campaignOverviewCalculator;
    private final OperationActionCalculator operationActionCalculator;
    private final OverviewStatusCalculator overviewStatusCalculator;

    /**
     * Mock 원천 조회와 캠페인·조치·전체 상태 계산에 필요한 협력 객체를 주입받습니다.
     *
     * @param timeProvider 테스트와 운영 환경에서 동일한 시간 계약을 제공하는 공통 공급자
     * @param mockDataFactory Repository 연결 전 캠페인 원천과 조치 후보를 제공하는 Factory
     * @param campaignOverviewCalculator 캠페인 상태·오픈 임박·V1 재고 계산기
     * @param operationActionCalculator 판정 완료 조치 후보의 KPI·목록 집계 계산기
     * @param overviewStatusCalculator 원천 상태를 전체 응답 완전성으로 계산하는 구성요소
     */
    public AdminOverviewService(
            TimeProvider timeProvider,
            AdminOverviewMockDataFactory mockDataFactory,
            CampaignOverviewCalculator campaignOverviewCalculator,
            OperationActionCalculator operationActionCalculator,
            OverviewStatusCalculator overviewStatusCalculator
    ) {
        this.timeProvider = timeProvider;
        this.mockDataFactory = mockDataFactory;
        this.campaignOverviewCalculator = campaignOverviewCalculator;
        this.operationActionCalculator = operationActionCalculator;
        this.overviewStatusCalculator = overviewStatusCalculator;
    }

    /**
     * 현재 시점의 관리자 운영현황을 반환합니다.
     *
     * <p>Mock 캠페인에서 계산할 수 있는 캠페인 상태·오픈 임박·V1 재고·조치 결과는 값과 관측 시각을
     * 제공하고, 발급·대기열·고객 결과처럼 원천이 없는 영역은 {@code UNAVAILABLE}로 함께 반환합니다.
     * 전체 상태는 조립된 모든 원천 상태를 기준으로 계산합니다.</p>
     *
     * @return Snapshot과 전체 데이터 완전성을 포함한 운영현황 Service 결과
     */
    public AdminOverviewResult getOverview() {
        // 한 응답 안의 시간 경계가 달라지지 않도록 기준 시각은 최초 한 번만 조회합니다.
        Instant snapshotAt = timeProvider.instant();
        AdminOverviewMockDataset dataset = mockDataFactory.create(snapshotAt);
        CampaignCalculation campaignCalculation = campaignOverviewCalculator.calculate(
                snapshotAt, dataset.campaigns());
        ActionCalculation actionCalculation = operationActionCalculator.calculate(
                dataset.actionCandidates());

        AdminOverviewSnapshot snapshot = new AdminOverviewSnapshot(
                snapshotAt,
                validObservation(actionCalculation.required(), snapshotAt),
                validObservation(campaignCalculation.openingSoon(), snapshotAt),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                validObservation(campaignCalculation.campaignStatusSummary(), snapshotAt),
                validObservation(actionCalculation.items(), snapshotAt),
                validObservation(campaignCalculation.campaigns(), snapshotAt),
                unavailableObservation()
        );
        return assemble(snapshot);
    }

    /**
     * 각 원천에서 계산된 운영 값과 전체 완전성을 하나의 Service 결과로 조립합니다.
     *
     * <p>후속 Repository·관측 조회가 준비되면 완성된 Snapshot을 이 경계로 전달합니다. 전체 완전성은
     * {@link OverviewStatusCalculator}에 위임하고, HTTP DTO 변환은 이 결과를 받는 Controller가
     * 담당합니다.</p>
     *
     * @param snapshot 캠페인·관측 원천별 계산이 끝난 기술 중립 결과
     * @return 계산된 Snapshot과 전체 완전성을 함께 보존한 Service 결과
     */
    public AdminOverviewResult assemble(AdminOverviewSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        OverallStatus overallStatus = overviewStatusCalculator.calculate(snapshot);
        return new AdminOverviewResult(snapshot, overallStatus);
    }

    /** 계산이 끝난 값을 공통 Core 관측 계약의 정상 상태와 기준 시각으로 감쌉니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> validObservation(
            T value,
            Instant observedAt
    ) {
        return new AdminOverviewSnapshot.Observation<>(value, SourceStatus.VALID, observedAt);
    }

    /** 실제로 관측하지 않은 독립 원천을 가짜 0 없이 공통 Core 상태 규칙에 맞춰 생성합니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> unavailableObservation() {
        return new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }
}
