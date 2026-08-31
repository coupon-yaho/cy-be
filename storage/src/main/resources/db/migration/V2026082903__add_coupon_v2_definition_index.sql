-- ⚠️ **번호가 V2026082901 에서 V2026082903 으로 밀렸다.** main 이 같은 날 같은 번호를
--    (V2026082901__issuance_status_id_index.sql) 쓰고 있어 머지에서 겹쳤다.
--    Flyway 는 "Found more than one migration with version 2026082901" 로 거절하고
--    앱이 아예 안 뜬다(실측). 아직 어느 배포에도 적용 안 된 파일이라 이름을 바꿨다.
--
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
