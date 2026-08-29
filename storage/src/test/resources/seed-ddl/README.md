# 시드 로더 DDL 의 읽기 전용 사본

`SchemaParityTest` 가 읽는다. **손으로 고치지 않는다.**

```
원본   coupon-yaho/cy-seed-data-generator @ 4307261  (2026-08-29)  ddl/
사본   이 디렉터리                                  바이트 동일
```

## 왜 사본인가

CI 는 옆 저장소를 못 읽는다. 그렇다고 대조를 포기하면 두 DDL 이 어긋나도 아무도 모른다 —
실제로 `datetime` ↔ `datetime(6)` 세 컬럼과 제약 이름 두 개가 그렇게 갈려 있었다.
`docs/contract.json` 과 같은 규율이다.

## 스키마 주인은 cy-be 다

`storage/src/main/resources/db/migration/` 의 Flyway 가 스키마를 **정의**한다.
시드는 300만 건을 빠르게 넣으려고 *테이블만 → 적재 → 제약* 순서로 파일을 쪼개는데,
그 **구조는 로더의 사정**이고 만들어지는 **최종 상태는 같아야** 한다.
`SchemaParityTest` 가 그 등식만 본다 — 파일 개수나 순서는 안 본다.

## 대조에서 빼는 것

| | 이유 |
|---|---|
| `90_perf_indexes_optional.sql` | `--with-perf-indexes` 전용 **처방전**이다. 보조 인덱스가 기본 스키마에 없는 것은 누락이 아니라 의도 — 300만 건에서 실행계획을 보고 인덱스를 처방해 개선폭을 재는 것이 과제의 일부다 |
| `BATCH_*` · `flyway_schema_history` | 잡 실행 이력이지 도메인이 아니다. `V11__batch_metadata.sql` 이 만들고 시드는 알 필요가 없다 |

## 갱신과 검증은 다른 일이다

한 명령으로 겸하면 차이가 났을 때 *사본을 손댄 것*인지 *원본이 바뀐 것*인지 구별할 수 없다.
둘 다 **먼저 임시 디렉터리에 받고, 성공했을 때만** 비교하거나 덮는다.

```bash
# 검증 — 기록된 리비전과 바이트 동일한가. 차이가 나면 사본을 손댄 것이다
#
# 파일명을 하드코딩하지 않는다. 상류에 파일이 하나 늘면 하드코딩된 목록은 그것을 안 받고,
# diff -r 은 양쪽에 없는 파일을 비교하지 않아 "같다" 를 출력한다.
set -euo pipefail
# 기준 리비전은 위 표에서 읽는다. 여기에 또 적으면 한쪽만 갱신돼 검증이
# "사본을 손댔다" 로 오진한다 — 실제로 그렇게 갈렸었다.
SHA=$(grep -oE '@ [0-9a-f]{7,40}' README.md | head -1 | cut -d' ' -f2)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

gh api "repos/coupon-yaho/cy-seed-data-generator/contents/ddl?ref=$SHA" \
  --jq '.[] | select(.name | endswith(".sql")) | .name' | while read -r f; do
  gh api "repos/coupon-yaho/cy-seed-data-generator/contents/ddl/$f?ref=$SHA" \
    --jq '.content' | base64 -d > "$tmp/$f"
done

# diff 를 마지막 명령으로 둔다. 뒤에 rm 을 붙이면 그 종료 코드가 diff 결과를 덮어
# CI 나 훅에 붙였을 때 사본이 갈라져도 통과한다 (trap 이 정리를 맡는다).
diff -r "$tmp" . --exclude=README.md
```

```bash
# 갱신 — 원본이 새 리비전으로 올라갔을 때만. 위 표의 SHA 도 같이 고친다
set -euo pipefail
SHA=<새 SHA>
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

gh api "repos/coupon-yaho/cy-seed-data-generator/contents/ddl?ref=$SHA" \
  --jq '.[] | select(.name | endswith(".sql")) | .name' | while read -r f; do
  gh api "repos/coupon-yaho/cy-seed-data-generator/contents/ddl/$f?ref=$SHA" \
    --jq '.content' | base64 -d > "$tmp/$f"
done

# 사본에서 사라진 파일도 반영해야 하므로 지우고 다시 깐다
rm -f ./*.sql && cp "$tmp"/*.sql .
```

갱신 뒤 `SchemaParityTest` 가 빨개지면 **시드가 앞서 나간 것**이다.
cy-be 가 주인이므로, 시드를 되돌리든 Flyway 마이그레이션을 추가하든 **한쪽을 맞춰야** 한다.
사본을 고쳐 초록으로 만드는 것은 대조를 없애는 것과 같다.

## 파일이 늘거나 줄면

`SchemaParityTestBase` 의 `CLEAN_DDL`·`CORRUPT_DDL`·`EXCLUDED_DDL` 중 하나에 넣어야 한다.
**사람의 규율에 맡기지 않는다** — `accountForEverySeedDdl` 이 이 디렉터리의 실제 내용과
세 목록의 합집합이 같은지 단언하므로, 새 파일을 어디에도 안 넣으면 테스트가 빨개진다.
제외한다면 왜인지 `EXCLUDED_DDL` javadoc 에 적는다.
