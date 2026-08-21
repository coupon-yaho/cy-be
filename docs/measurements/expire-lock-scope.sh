#!/bin/bash
# 만료 배치 EXPIRE_BATCH 가 잡는 락 범위와, 그것이 발급·사용 경로에 미치는 영향을 잰다.
#
#   재현:  bash docs/measurements/expire-lock-scope.sh
#   결과:  docs/12-expire-lock-measurement.md
#
# 도커만 있으면 돈다. 컨테이너는 끝나면 지워진다.
# 실제 운영 테이블(300만 행) 대신 5,000행으로 재고, 비례하는 축만 본다 —
# 락 수는 스캔 행 수를 따라가고, 발급 INSERT 통과 여부는 행 수와 무관하다.
set -u
C=cy-m-$$
ROWS=5000; LIMIT=1000; WINDOW=1000
docker run -d --name $C -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=t \
  mysql:latest --skip-log-bin >/dev/null
trap "docker rm -f $C >/dev/null 2>&1" EXIT
for i in $(seq 90); do docker exec $C mysql -proot -e "SELECT 1" >/dev/null 2>&1 && break; sleep 2; done
echo "서버: $(docker exec $C mysqld --version 2>/dev/null | sed 's/.*Ver //;s/ for.*//')  ·  행 $ROWS · LIMIT $LIMIT · 스캔창 $WINDOW"
docker exec -i $C mysql -proot t >/dev/null 2>&1 <<SQL
SET GLOBAL innodb_lock_wait_timeout = 3;
CREATE TABLE issuances (id bigint PRIMARY KEY AUTO_INCREMENT, coupon_id bigint,
  status varchar(12), expires_at datetime(6), updated_at datetime(6));
SQL
load() {  # $1 = 만료 대상 건수
  docker exec -i $C mysql -proot -N t >/dev/null 2>&1 <<SQL
SET SESSION cte_max_recursion_depth = 100000;
TRUNCATE issuances;
INSERT INTO issuances (id, coupon_id, status, expires_at, updated_at)
WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < $ROWS)
SELECT n, 1, IF(n <= $1, 'ISSUED', 'USED'), '2020-01-01', '2020-01-01' FROM s;
SQL
  local n; n=$(docker exec -i $C mysql -proot -N t 2>/dev/null <<<"SELECT COUNT(*) FROM issuances")
  [ "$n" = "$ROWS" ] || { echo "적재 실패 $n/$ROWS"; exit 1; }
}
probe() {  # $1=대상 $2=인덱스 $3=격리 $4=스캔창 $5=라벨
  load "$1"
  docker exec -i $C mysql -proot -N t >/dev/null 2>&1 <<<"DROP INDEX idx_status_expires ON issuances"
  [ "$2" = "yes" ] && docker exec -i $C mysql -proot -N t >/dev/null 2>&1 \
    <<<"CREATE INDEX idx_status_expires ON issuances (status, expires_at)"
  local UP=""; [ "$4" = "yes" ] && UP="AND id <= $WINDOW"
  docker exec -i $C mysql -proot -N t > /tmp/pr_$$.txt 2>&1 <<SQL &
SET SESSION TRANSACTION ISOLATION LEVEL $3;
START TRANSACTION;
UPDATE issuances SET status='EXPIRED', updated_at='2026-01-15 09:03:00'
 WHERE status='ISSUED' AND expires_at < '2026-08-19' AND id > 0 $UP ORDER BY id LIMIT $LIMIT;
SELECT CONCAT('m=', ROW_COUNT());
SELECT CONCAT('l=', COUNT(*)) FROM performance_schema.data_locks
 WHERE OBJECT_NAME='issuances' AND LOCK_TYPE='RECORD' AND THREAD_ID=PS_CURRENT_THREAD_ID();
SELECT SLEEP(8);
COMMIT;
SQL
  sleep 3
  local ins use
  ins=$(docker exec -i $C mysql -proot -N t 2>&1 <<SQL | grep -c "ERROR 1205"
SET SESSION innodb_lock_wait_timeout = 2;
INSERT INTO issuances (coupon_id,status,expires_at,updated_at) VALUES (1,'ISSUED','2027-01-01','2026-01-01');
SQL
)
  use=$(docker exec -i $C mysql -proot -N t 2>&1 <<SQL | grep -c "ERROR 1205"
SET SESSION innodb_lock_wait_timeout = 2;
UPDATE issuances SET status='USED' WHERE id = 4500;
SQL
)
  wait
  printf "%-30s 매치=%-5s 락=%-6s 발급INSERT=%-5s 사용UPDATE=%s\n" "$5" \
    "$(grep -o 'm=[0-9]*' /tmp/pr_$$.txt | cut -d= -f2)" \
    "$(grep -o 'l=[0-9]*' /tmp/pr_$$.txt | cut -d= -f2)" \
    "$([ "$ins" = "0" ] && echo 통과 || echo 1205)" \
    "$([ "$use" = "0" ] && echo 통과 || echo 1205)"
  rm -f /tmp/pr_$$.txt
}
echo; echo "── 만료 대상 0건 (하루 288회 중 대부분) ──"
probe 0 no  "REPEATABLE READ" no  "현재 상태"
probe 0 no  "REPEATABLE READ" yes "+ 스캔창"
probe 0 yes "REPEATABLE READ" no  "+ 인덱스"
probe 0 yes "REPEATABLE READ" yes "+ 인덱스 + 스캔창"
probe 0 no  "READ COMMITTED"  no  "+ READ COMMITTED"
probe 0 yes "READ COMMITTED"  no  "+ 인덱스 + RC"
echo; echo "── 매치 300건 (LIMIT 미달 = 모든 실행의 마지막 청크) ──"
probe 300 no  "REPEATABLE READ" no  "현재 상태"
probe 300 no  "REPEATABLE READ" yes "+ 스캔창"
probe 300 yes "REPEATABLE READ" no  "+ 인덱스"
probe 300 no  "READ COMMITTED"  no  "+ READ COMMITTED"
probe 300 yes "READ COMMITTED"  no  "+ 인덱스 + RC"
echo; echo "── 매치 1000건 (LIMIT 충족) ──"
probe 1000 no  "REPEATABLE READ" no  "현재 상태"
probe 1000 yes "REPEATABLE READ" no  "+ 인덱스"
probe 1000 yes "READ COMMITTED"  no  "+ 인덱스 + RC"

# ── 스캔 축 ─────────────────────────────────────────────────────────────────
# 락이 아니라 **읽은 행 수**를 잰다. READ COMMITTED 로 내린 뒤로는 매치 안 된 행의 락을
# 즉시 놓으므로 인덱스가 없어도 락이 0 이다 — 그 축으로는 인덱스를 지킬 수 없다.
# docs/12 §4 · §5 의 수치가 여기서 나온다.
scan_probe() {  # $1=대상 $2=인덱스 $3=라벨 $4=행수(기본 ROWS)
  local rows=${4:-$ROWS}
  docker exec -i $C mysql -proot -N t >/dev/null 2>&1 <<SQL
SET SESSION cte_max_recursion_depth = 1000000;
TRUNCATE issuances;
INSERT INTO issuances (id, coupon_id, status, expires_at, updated_at)
WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < $rows)
SELECT n, 1, IF(n > $rows - $1, 'ISSUED', 'USED'), '2020-01-01', '2020-01-01' FROM s;
SQL
  docker exec -i $C mysql -proot -N t >/dev/null 2>&1 <<<"DROP INDEX idx_status_expires ON issuances"
  [ "$2" = "yes" ] && docker exec -i $C mysql -proot -N t >/dev/null 2>&1 \
    <<<"CREATE INDEX idx_status_expires ON issuances (status, expires_at)"
  docker exec -i $C mysql -proot -N t 2>/dev/null <<SQL | tr '\n' ' ' | sed "s/^/$3  /"
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
FLUSH STATUS;
UPDATE issuances SET status='EXPIRED', updated_at='2026-01-15 09:03:00'
 WHERE status='ISSUED' AND expires_at < '2026-08-19' AND id > 0 ORDER BY id LIMIT $LIMIT;
SELECT CONCAT('넘김=', ROW_COUNT());
SELECT CONCAT('읽은행=', SUM(VARIABLE_VALUE)) FROM performance_schema.session_status
 WHERE VARIABLE_NAME IN ('Handler_read_next','Handler_read_rnd_next','Handler_read_first','Handler_read_key');
SELECT CONCAT('정렬=', VARIABLE_VALUE) FROM performance_schema.session_status
 WHERE VARIABLE_NAME='Sort_rows';
SQL
  echo
}

echo; echo "── 스캔 축 · 만료 대상 0건 (인덱스가 지키는 것) ──"
scan_probe 0 no  "인덱스 없음:"
scan_probe 0 yes "인덱스 있음:"

echo; echo "── 스캔 축 · 200,000행 중 뒤 1,000건만 대상 (백로그 최악) ──"
scan_probe 1000 no  "인덱스 없음:" 200000
scan_probe 1000 yes "인덱스 있음:" 200000
