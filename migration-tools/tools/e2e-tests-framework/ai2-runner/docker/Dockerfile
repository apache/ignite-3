FROM eclipse-temurin:11-jdk

RUN mkdir -p /opt/app
WORKDIR /opt/app

COPY libs /opt/app/libs

ENV CONFIG_URI="/opt/app/config.xml"

CMD ["java", "-cp", "libs/*", "org.junit.platform.console.ConsoleLauncher", "execute", "--fail-if-no-tests", "--details", "verbose", "-c", "org.apache.ignite.migrationtools.tests.e2e.framework.runners.Ignite2SeedDataTest"]