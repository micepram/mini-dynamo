# Multi-stage: build the bootJar with the wrapper, run it on a JRE. Self-contained so both
# `docker compose` and the Testcontainers integration test can build it from the repo.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY . .
RUN ./gradlew bootJar --no-daemon -x test

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
