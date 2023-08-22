# Apache Ignite 3 OpenTelemetry extension

1. Prepare extension jar
`$ gradle clean build`
2. Download OpenTelemetry java agent v.1.29.0 required for code auto instrumentation
https://opentelemetry.io/docs/instrumentation/java/automatic/#setup
3. Launch ignite with additional java parameters
`-javaagent:{pathToJavaAgent}/opentelemetry-javaagent-1.29.0.jar -Dotel.javaagent.extensions=ignite-3/modules/otel-ext/build/libs/ignite-otel-ext-3.0.0-SNAPSHOT.jar`
and configured span exporter
https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#span-exporters
For example Zipkin:
`-Dotel.traces.exporter=zipkin -Dotel.exporter.zipkin.endpoint=http://localhost:9411/api/v2/spans`
