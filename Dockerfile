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
