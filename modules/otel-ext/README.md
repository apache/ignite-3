# Apache Ignite 3 OpenTelemetry extension

By default all traces will be exported by using Zipkin exporter and will be send by http://localhost:9411/api/v2/spans

To override endpoint need to pass following system properties:
`-Dotel.exporter.zipkin.endpoint=http://host:9411/api/v2/spans`
or using environment variables:
`OTEL_EXPORTER_ZIPKIN_ENDPOINT=http://host:9411/api/v2/spans`

To enable export, you need to set the sampling rate:
```java
node.clusterConfiguration().getConfiguration(TracingConfiguration.KEY).change(change -> {
    change.changeRatio(0.5d);
});
```

# Exporting traces to the files in zipkin format

All trace files will be placed into <work folder path> in JSON format with name `<traceId>.json`

To enable need to pass following system properties:
`-Dotel.traces.exporter=file-zipkin -Dotel.exporter.file-zipkin.base-path=<work folder path>`
or using environment variables:
`OTEL_TRACES_EXPORTER=file-zipkin OTEL_EXPORTER_FILE_ZIPKIN_BASE_PATH=<work folder path>`

NOTE: These settings must be set on all nodes.

# Exporting traces to the jaeger

To enable need to pass following system properties:
`-Dotel.traces.exporter=otel -Dotel.exporter.otlp.endpoint=http://host:9411/api/v2/spans`
or using environment variables:
`OTEL_TRACES_EXPORTER=otel OTEL_EXPORTER_OTLP_ENDPOINT=http://host:9411/api/v2/spans`

NOTE: These settings must be set on all nodes.


