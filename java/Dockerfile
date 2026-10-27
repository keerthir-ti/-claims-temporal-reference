# Java application (Lifecycle + STP workers, event bridge). Build context: repository root.
#   docker build -f java/Dockerfile .                -> runtime image (lifecycle, stp, bridge roles)
#   docker build -f java/Dockerfile --target test .  -> runs the Java test suite during the build
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY contracts contracts
COPY java/pom.xml java/pom.xml
RUN cd java && mvn -q -B dependency:resolve dependency:resolve-plugins || true
COPY java/src java/src
RUN cd java && mvn -q -B -DskipTests package

FROM build AS test
COPY config config
RUN cd java && mvn -B test

FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app
COPY --from=build /src/java/target/claims-temporal-java.jar /app/app.jar
COPY config /app/config
ENV FLOWS_DIR=/app/config/flows
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
