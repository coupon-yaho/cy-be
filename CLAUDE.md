# coupon-yaho

## 검증 범위 — 전체 빌드를 돌리지 않는다

이 저장소의 `./gradlew clean build` 는 Testcontainers 때문에 **회당 4~5분**이다.
코드를 고친 뒤에는 **손댄 모듈과 테스트만** 지정해 돌린다.

```
✓  ./gradlew :infra:mq:test --tests '*KafkaBrokerComposeContractTest*'
✓  ./gradlew :api:test --tests '*CampaignMeter*'
✓  ./gradlew :core:test
✗  ./gradlew build
✗  ./gradlew clean build
✗  ./gradlew test
```

모듈은 `:api` `:batch` `:core` `:storage` `:infra:mq` `:infra:redis` 여섯이다.
모듈 경로 없는 `build` · `test` · `check` · `clean` 은 훅(`.claude/hooks/gradle-scope-guard.sh`)이
차단한다. 전체 빌드가 정말 필요하면 그 사실을 말하고 사용자가 직접 돌리게 한다.

여러 모듈을 함께 봐야 하면 나열한다 — `./gradlew :core:test :api:test`.
가드 테스트를 일부러 깨뜨려 확인하는 절차도 해당 테스트 클래스만 지정해 돌린다.

바꾼 모듈이 어디에 걸리는지 모르겠으면 의존 방향으로 판단한다:
`core` ← `storage` · `infra:*` ← `api` · `batch`. `core` 를 고쳤으면 그 위 모듈 중
실제로 그 코드를 쓰는 것만 더 돌린다. 전부 돌리는 것과 같아지면 안 된다.
