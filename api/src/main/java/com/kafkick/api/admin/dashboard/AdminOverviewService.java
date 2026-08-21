package com.kafkick.api.admin.dashboard;

import java.time.Instant;
import java.util.Objects;

import org.springframework.stereotype.Service;

import com.kafkick.api.admin.dashboard.AdminOverviewResult.OverallStatus;
import com.kafkick.api.admin.dashboard.calculator.OverviewStatusCalculator;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/**
 * 관리자 첫 화면에 필요한 운영현황 조회와 결과 조립 흐름을 담당합니다.
 *
 * <p>후속 캠페인 Repository와 B 관측 조회 구성요소가 준비되면 이 Service에 직접 연결하고,
 * 원천별 결과를 Calculator에 전달해 {@link AdminOverviewResult}를 생성합니다. 계산식과 정책 판정은
 * 전용 Calculator가 담당하며 Service는 조회 순서와 결과 조립만 조정합니다.</p>
 *
 * <p>현재는 실제 운영 원천이 연결되지 않았으므로 관측하지 않은 수치를 0으로 위조하지 않고
 * 미수집 {@link AdminOverviewSnapshot}을 생성합니다. 이 상태에서도 {@code snapshotAt}은 원천 관측
 * 시각이 아니라 Service가 결과를 조립한 시각으로 제공합니다.</p>
 */
@Service
public class AdminOverviewService {

    private final TimeProvider timeProvider;
    private final OverviewStatusCalculator overviewStatusCalculator;

    /**
     * 조회 시각과 현재 Service 흐름에서 사용하는 전체 상태 계산기를 주입받습니다.
     *
     * <p>조치 후보 집계는 실제 후보 원천이 아직 연결되지 않았으므로 Service의 불필요한 의존성으로
     * 추가하지 않습니다. 후속 후보 생성 흐름이 구현되면
     * {@link com.kafkick.api.admin.dashboard.calculator.OperationActionCalculator}를 이 생성자에 추가하고
     * {@code getOverview()}에서 사용합니다.</p>
     *
     * @param timeProvider 테스트와 운영 환경에서 동일한 시간 계약을 제공하는 공통 공급자
     * @param overviewStatusCalculator 원천 상태를 전체 응답 완전성으로 계산하는 구성요소
     */
    public AdminOverviewService(
            TimeProvider timeProvider,
            OverviewStatusCalculator overviewStatusCalculator
    ) {
        this.timeProvider = timeProvider;
        this.overviewStatusCalculator = overviewStatusCalculator;
    }

    /**
     * 현재 시점의 관리자 운영현황을 반환합니다.
     *
     * <p>후속 원천 연결 전에는 모든 관측 영역을 {@code UNAVAILABLE}로 반환합니다. 캠페인 Repository와
     * 관측 조회가 구현되면 이 메서드에서 명시적 결과 타입으로 원천을 조회하고 각 Calculator를 호출한
     * 뒤 Snapshot을 조립합니다.</p>
     *
     * @return Snapshot과 전체 데이터 완전성을 포함한 운영현황 Service 결과
     */
    public AdminOverviewResult getOverview() {
        Instant snapshotAt = timeProvider.instant();
        AdminOverviewSnapshot snapshot = unavailableSnapshot(snapshotAt);
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

    /** 운영 원천 미연결 상태를 가짜 0 없이 Core Snapshot으로 표현합니다. */
    private static AdminOverviewSnapshot unavailableSnapshot(Instant snapshotAt) {
        return new AdminOverviewSnapshot(
                snapshotAt,
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation(),
                unavailableObservation()
        );
    }

    /** 실제로 관측하지 않은 독립 원천을 공통 Core 상태 규칙에 맞춰 생성합니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> unavailableObservation() {
        return new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }
}
