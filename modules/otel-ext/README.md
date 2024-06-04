# Apache Ignite 3 OpenTelemetry extension

By default all traces will be exported by using Zipkin exporter and will be send by http://localhost:9411/api/v2/spans

To enable export, you need to set the sampling rate:
```java
node.clusterConfiguration().getConfiguration(TracingConfiguration.KEY).change(change -> {
    change.changeRatio(0.5d);
});
```



