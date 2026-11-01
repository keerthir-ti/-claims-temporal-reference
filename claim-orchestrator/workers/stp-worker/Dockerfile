# STP worker image (Java): run as 'stp' or 'claimspro'. Build context: claim-orchestrator/.
#   docker build -f workers/stp-worker/Dockerfile .               runtime image
#   docker build -f workers/stp-worker/Dockerfile --target test .  runs the tests
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /src
COPY pom.xml .
COPY contracts contracts
COPY workers/worker-common workers/worker-common
COPY workers/lifecycle-worker workers/lifecycle-worker
COPY workers/stp-worker workers/stp-worker
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl workers/stp-worker -am package -DskipTests

FROM build AS test
COPY config config
RUN --mount=type=cache,target=/root/.m2 mvn -B -pl workers/stp-worker -am verify

FROM eclipse-temurin:25-jre AS runtime
WORKDIR /app
COPY --from=build /src/workers/stp-worker/target/app.jar /app/app.jar
COPY config /app/config
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
