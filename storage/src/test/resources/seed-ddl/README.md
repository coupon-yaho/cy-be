# 시드 로더 DDL 의 읽기 전용 사본

`SchemaParityTest` 가 읽는다. **손으로 고치지 않는다.**

```
원본   coupon-yaho/cy-seed-data-generator @ 4d1a2a0  (2026-08-17)  ddl/
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
| `BATCH_*` · `flyway_schema_history` | 잡 실행 이력이지 도메인이 아니다. `V2__batch_metadata.sql` 이 만들고 시드는 알 필요가 없다 |

## 갱신과 검증은 다른 일이다

한 명령으로 겸하면 차이가 났을 때 *사본을 손댄 것*인지 *원본이 바뀐 것*인지 구별할 수 없다.
둘 다 **먼저 임시 디렉터리에 받고, 성공했을 때만** 비교하거나 덮는다.

```bash
# 검증 — 기록된 리비전과 바이트 동일한가. 차이가 나면 사본을 손댄 것이다
set -o pipefail
tmp=$(mktemp -d)
SHA=4d1a2a0
for f in 00_schema 10_constraints_common 11_constraints_clean \
         12_constraints_corrupt 90_perf_indexes_optional; do
  gh api "repos/coupon-yaho/cy-seed-data-generator/contents/ddl/$f.sql?ref=$SHA" \
    --jq '.content' | base64 -d > "$tmp/$f.sql" || exit 1
done
diff -r "$tmp" . --exclude=README.md && echo "사본이 원본과 같다"
rm -rf "$tmp"
```

```bash
# 갱신 — 원본이 새 리비전으로 올라갔을 때만. 위 표의 SHA 도 같이 고친다
set -o pipefail
tmp=$(mktemp -d)
SHA=<새 SHA>
for f in 00_schema 10_constraints_common 11_constraints_clean \
         12_constraints_corrupt 90_perf_indexes_optional; do
  gh api "repos/coupon-yaho/cy-seed-data-generator/contents/ddl/$f.sql?ref=$SHA" \
    --jq '.content' | base64 -d > "$tmp/$f.sql" || exit 1
done
cp "$tmp"/*.sql . && rm -rf "$tmp"
```

갱신 뒤 `SchemaParityTest` 가 빨개지면 **시드가 앞서 나간 것**이다.
cy-be 가 주인이므로, 시드를 되돌리든 Flyway 마이그레이션을 추가하든 **한쪽을 맞춰야** 한다.
사본을 고쳐 초록으로 만드는 것은 대조를 없애는 것과 같다.

## 파일이 늘거나 줄면

`SchemaParityTest.CLEAN_DDL` 목록도 같이 고친다. 목록에 없는 파일은 조용히 무시되므로,
새 DDL 파일이 생겼는데 목록에 안 넣으면 **그 파일의 내용은 대조되지 않는다.**
