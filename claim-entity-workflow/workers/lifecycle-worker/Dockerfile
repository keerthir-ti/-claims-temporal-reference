# Lifecycle worker (Java). Build context: claim-orchestrator/.
#   docker build -f workers/lifecycle-worker/Dockerfile .               runtime image
#   docker build -f workers/lifecycle-worker/Dockerfile --target test .  runs the tests
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /src
COPY pom.xml .
COPY contracts contracts
COPY worker-runtime/java worker-runtime/java
COPY workers/lifecycle-worker workers/lifecycle-worker
COPY workers/stp-worker workers/stp-worker
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl workers/lifecycle-worker -am package -DskipTests

FROM build AS test
COPY config config
RUN --mount=type=cache,target=/root/.m2 mvn -B -pl workers/lifecycle-worker -am verify

FROM eclipse-temurin:25-jre AS runtime
WORKDIR /app
COPY --from=build /src/workers/lifecycle-worker/target/app.jar /app/app.jar
COPY config /app/config
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
