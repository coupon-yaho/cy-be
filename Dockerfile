# 두 앱(api·batch)을 같은 파일로 짓습니다. ARG APP_MODULE 이 무엇을 지을지 정합니다.
#
# ⚠️ **기본값이 api 다.** batch 를 지으려면 compose 가 build.args 로 APP_MODULE=batch 를
#    줘야 한다 — 안 주면 batch 서비스가 api jar 를 담은 이미지로 뜬다(batch.yml 이 준다).
#
# 배치가 컨테이너여야 하는 이유: infra/prometheus/prometheus.yml 의 스크레이프 대상이
# batch:9092 이고, 그 DNS 이름을 만드는 것은 compose 서비스뿐이다. 앱이 호스트 JVM 이면
# 관제 컨테이너에서 이름 해석이 안 된다.

# syntax=docker/dockerfile:1.7

FROM eclipse-temurin:21-jdk-jammy AS builder

WORKDIR /workspace

COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
COPY api/build.gradle api/build.gradle
COPY batch/build.gradle batch/build.gradle
COPY core/build.gradle core/build.gradle
COPY storage/build.gradle storage/build.gradle
COPY infra/mq/build.gradle infra/mq/build.gradle
COPY infra/redis/build.gradle infra/redis/build.gradle

RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew dependencies --no-daemon >/dev/null

COPY api/src api/src
COPY batch/src batch/src
COPY core/src core/src
COPY storage/src storage/src
COPY infra/mq/src infra/mq/src
COPY infra/redis/src infra/redis/src

ARG APP_MODULE=api
RUN --mount=type=cache,target=/root/.gradle \
    find . -path '*/src/main/resources/*.yml.example' \
      -exec sh -c 'cp "$1" "${1%.example}"' _ {} \; \
    && ./gradlew ":${APP_MODULE}:bootJar" --no-daemon \
    && cp "${APP_MODULE}"/build/libs/*.jar /workspace/application.jar

FROM eclipse-temurin:21-jre-jammy

RUN useradd --system --uid 10001 --create-home app
WORKDIR /app

COPY --from=builder --chown=app:app /workspace/application.jar application.jar

USER app
EXPOSE 8080 9090
ENTRYPOINT ["java", "-jar", "/app/application.jar"]
