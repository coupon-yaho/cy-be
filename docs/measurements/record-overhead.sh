#!/bin/bash
# 독립 요청 기록(CY-960)이 부하 도구 자체를 느리게 하는지 잰다.
#
#   재현:  bash docs/measurements/record-overhead.sh
#   결과:  perf/README.md §4.1
#
# 도커만 있으면 돈다. 컨테이너와 네트워크는 끝나면 지워진다. 12분쯤 걸린다 —
# k6 한 회차마다 워밍업 1초 + 시작 대기 12초 + 측정 3초 + gracefulStop 이 붙는다.
#
# **뒤를 nginx 로 세운다.** 앞선 측정에서 파이썬 ThreadingTCPServer 를 두었다가
# 그것 자체가 상한이 돼 모든 모드가 같아 보인 적이 있다. 서버가 병목이면 기록의
# 비용이 그 뒤에 숨는다.
#
# ⚠️ **12,000/s 위로는 이 계기로 못 잰다.** 그 위에서는 수가 스스로 모순됐다 —
#    설정 20,000/s 에서 "못 쏜 것 0" 인데 예정 40,000 중 6,180만 실행됐고,
#    32,000/s 에서는 0.5/s 가 나왔다. 도커에 코어 11개가 다 붙어 있는데도 그렇다.
#    맥북에서 도커로 도는 k6 가 그 도착률을 못 낸다는 뜻이라, 그 구간의 끔/켬
#    비교는 신뢰할 수 없다. **그래서 여기서는 그 구간을 재지 않는다.**
# 실패하면 바로 멈춘다. 형제 측정 스크립트는 `set -u` 만 쓰는데, 그쪽은 일부러
# 실패시키는 프로브(락 타임아웃)가 있어서다. 여기서는 실패할 예정인 명령이 없고,
# k6 한 회차가 죽어도 뒤 회차가 성공하면 스크립트가 0 으로 끝나 **표에 줄 하나가
# 빠진 것을 못 보고 지나친다.**
set -euo pipefail
NET=cy963net-$$
BE=cy963be-$$
WORK=$(mktemp -d)
trap 'docker rm -f "$BE" >/dev/null 2>&1; docker network rm "$NET" >/dev/null 2>&1; rm -rf "$WORK"' EXIT

cat > "$WORK/nginx.conf" <<'EOF'
worker_processes auto;
events { worker_connections 20480; }
http {
  access_log off;
  keepalive_timeout 75s;
  keepalive_requests 1000000;
  server {
    listen 8080 backlog=20480 reuseport;
    location / {
      add_header Content-Type application/json always;
      return 201 '{"success":true,"data":{"issuanceId":777,"couponRoundId":1,"code":"A","status":"ISSUED"}}';
    }
  }
}
EOF
cp "$(dirname "$0")/../../perf/k6/issue.js" "$WORK/issue.js"

docker network create "$NET" >/dev/null
docker run -d --name "$BE" --network "$NET" \
  -v "$WORK/nginx.conf:/etc/nginx/nginx.conf:ro" nginx:1.27-alpine >/dev/null
sleep 3

# 한 회차. 기록 끔/켬만 다르다.
run() {  # $1=도착률 $2=기록 $3=라벨
  docker run --rm --network "$NET" -v "$WORK:/w" -w /w grafana/k6:latest run issue.js \
    -e "BASE_URL=http://$BE:8080" -e WARMUP_ROUND_ID=9001 -e TARGET_ROUND_ID=7001 \
    -e WARMUP_RATE=1 -e WARMUP_SECONDS=1 -e "TARGET_RATE=$1" -e TARGET_SECONDS=3 \
    -e MEMBER_BASE=1 -e WARMUP_MEMBER_BASE=1 -e MEMBER_GRADE=VIP -e WARMUP_MEMBER_GRADE=VIP \
    -e HTTP_TIMEOUT=60s -e "OUT_JSON=/w/s-$3.json" -e "RECORD_REQUESTS=$2" \
    --log-format=raw --console-output="/w/req-$3.log" >/dev/null 2>&1
  python3 - "$WORK/s-$3.json" "$WORK/req-$3.log" "$3" "$2" <<'PY'
import json, os, sys
d = json.load(open(sys.argv[1]))
attempts = d["perf"]["measure_attempts"]
dropped = int(d["metrics"].get("dropped_iterations", {}).get("values", {}).get("count", 0))
# 기록은 요청마다 두 줄(REQ + 결과)이고, 맨 앞에 회차 시작 표식 한 줄이 더 있다.
# 그 수가 안 맞으면 줄이 샜다는 뜻이라, 도착률만 보고 "괜찮다" 고 하면 안 된다.
lines = sum(1 for l in open(sys.argv[2])) if os.path.exists(sys.argv[2]) else 0
state = "-" if sys.argv[4] == "false" else (
    "완전" if lines == 2 * attempts + 1 else f"어긋남 {lines} vs {2 * attempts + 1}")
print(f"{sys.argv[3]:>10}  시도 {attempts:>6}  달성 {d['perf']['achieved_arrival_rps']:>8.1f}/s"
      f"  못쏨 {dropped:>4}  기록 {state}")
PY
}

echo "① 도착률을 올려 가며 — 갈라지는 지점이 있나"
for rate in 1000 3000 6667; do
  run "$rate" false "off-$rate"
  run "$rate" true  "on-$rate"
done

# 한 쌍만 보고 "차이 없다" 고 할 수 없다. 같은 조건의 흔들림보다 차이가 작으면
# 그 차이는 없는 것이다. **번갈아** 돌려 시간에 따른 표류가 한쪽에 몰리지 않게 한다.
echo
echo "② spike 프로필의 실제 도착률(6667/s)에서 끔·켬 번갈아 3쌍 — 흔들림의 폭을 본다"
for i in 1 2 3; do
  run 6667 false "off-r$i"
  run 6667 true  "on-r$i"
done
