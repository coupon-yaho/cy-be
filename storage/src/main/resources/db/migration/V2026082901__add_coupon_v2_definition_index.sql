-- V2 정의 목록 조회 전용 인덱스.
--
-- 질의는 issuance_engine_version = 'V2' AND status IN ('SCHEDULED','OPEN') AND close_at > :asOf 다.
-- 세 컬럼에 인덱스가 없으면 coupons 전체를 훑는다. 지금 규모(수백 행)에서는 티가 안 나지만,
-- 이 질의에는 호출자 예산 100ms 가 붙어 있어 스캔 비용이 늘면 곧바로 완화 응답(503)이 된다.
-- 특히 첫 로드는 콜드 버퍼풀 위에서 도는 풀스캔이라 콜드 스타트에서 가장 먼저 드러난다.
--
-- 순서는 선택도 순이다. 등가 조건 둘(engine, status)이 앞이고 범위 조건(close_at)이 뒤다 —
-- 범위를 앞에 두면 그 뒤 컬럼은 인덱스로 못 좁힌다.
CREATE INDEX ix_coupons_v2_definition
    ON coupons (issuance_engine_version, status, close_at);
