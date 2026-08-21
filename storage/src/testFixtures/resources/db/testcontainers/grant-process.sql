-- 테스트 계정에 PROCESS 를 준다. performance_schema.data_locks 를 읽기 위해서다.
--
-- 만료 배치의 가장 큰 위험이 "무엇을 넘겼나" 가 아니라 "무엇을 잠갔나" 다.
-- 거를 인덱스가 없으면 넘길 것이 없는 실행조차 훑은 행을 전부 X 락으로 잡고,
-- 그동안 만료와 무관한 발급·사용이 막힌다. 그것을 실제로 재는 방법이 data_locks 뿐이라
-- ExpirationLockScopeTest 의 락 범위 테스트들이 이 권한에 기댄다.
--
-- PROCESS 는 읽기 전용 관측 권한이다. 데이터를 바꾸지 못하므로 다른 테스트가
-- 운영보다 느슨한 권한에서 도는 문제는 생기지 않는다.
--
-- 이 파일은 컨테이너의 /docker-entrypoint-initdb.d/ 로 복사되어 **root 로** 실행된다.
-- 계정 생성 뒤에 도는 자리라 GRANT 대상이 이미 존재한다. 유저명은 MySqlContainerConfig 의
-- withUsername 과 같아야 한다 — 어긋나면 컨테이너가 뜨지 않아 그 자리에서 드러난다.
--
-- **둘 다 있어야 한다.** PROCESS 만 주면 SHOW GRANTS 에는 멀쩡히 보이는데 조회는
-- "SELECT command denied ... for table 'data_locks'" 로 막힌다. mysql:latest 에 재 봤다.

GRANT PROCESS ON *.* TO 'test'@'%';
GRANT SELECT ON performance_schema.* TO 'test'@'%';
