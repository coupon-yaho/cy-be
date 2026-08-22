# 배치 서버 이미지입니다. 관제가 컨테이너 이름으로 스크레이프하므로 앱도 컨테이너여야 합니다.
#
# infra/prometheus/prometheus.yml 의 스크레이프 대상이 batch:9092 다. batch 는 DNS 이름이고
# 그 이름을 만드는 것은 compose 서비스뿐이라, 앱이 호스트 JVM 이면 컨테이너에서 해석이 안 된다.
# application.yml.example 도 "관제의 스크레이프 대상은 컨테이너 기준" 이라고 적어 뒀다.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src

# 래퍼와 빌드 스크립트를 먼저 넣어 의존성 계층을 캐시한다. 소스만 바뀌면 이 계층은 그대로다.
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
COPY core/build.gradle core/
COPY storage/build.gradle storage/
COPY batch/build.gradle batch/
COPY api/build.gradle api/
COPY infra infra
RUN ./gradlew --no-daemon :batch:dependencies --quiet || true

COPY core core
COPY storage storage
COPY batch batch
COPY api api

# README 가 정한 절차 그대로다 — .example 을 실제 이름으로 복사한다.
# resolved/ 는 processTestResources 가 만드는 테스트 전용이라 런타임에는 없다.
RUN find . -path '*/src/main/resources/*.yml.example' \
      -exec sh -c 'cp "$1" "${1%.example}"' _ {} \;

RUN ./gradlew --no-daemon :batch:bootJar -x test

FROM eclipse-temurin:21-jre
WORKDIR /app

# 루트로 돌리지 않는다. 이미지가 하는 일은 잡을 돌리는 것뿐이라 쓸 권한이 필요 없다.
RUN useradd --system --create-home --shell /usr/sbin/nologin batch
USER batch

COPY --from=build --chown=batch:batch /src/batch/build/libs/*.jar app.jar

# 업무 포트만 노출한다. 관리 포트(9092)는 compose 내부 네트워크에서만 닿는다 —
# 호스트로 매핑하면 인증 없이 전 지표가 열린다.
EXPOSE 9090

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
