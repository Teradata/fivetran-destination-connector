FROM openjdk:11-jre-slim

EXPOSE 50052

RUN mkdir /app

COPY build/libs/*.jar /app/JavaDestination.jar

ENTRYPOINT ["java", "-jar", "--illegal-access=debug", "--add-opens=java.base/java.io=ALL-UNNAMED", "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.nio.charset=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED", "/app/JavaDestination.jar"]
