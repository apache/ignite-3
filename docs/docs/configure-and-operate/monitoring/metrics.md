---
id: metrics
title: Metrics
sidebar_label: Overview
---

Apache Ignite 3 provides metrics for monitoring cluster health, performance, and resource utilization. Metrics help you understand system behavior, identify performance issues, and optimize cluster operations.

## Metrics Architecture

Ignite organizes metrics into sources that represent different subsystems or components. Each metric source can be enabled or disabled independently to control collection overhead. Collected metrics are exposed through exporters that integrate with monitoring tools.

Key components:

- **Metric Sources**: Logical groupings of related metrics (JVM, storage, SQL, etc.)
- **Metric Exporters**: Integration points for monitoring tools (JMX, OpenTelemetry, logs)
- **System Views**: SQL interface for querying metrics directly

## Working with Metrics

### Configuration

Control metric collection and export behavior through CLI commands. By default, metric sources are disabled to minimize performance impact. Enable only the sources you need for monitoring.

For configuration details, see [Configuring Metrics](configuring-metrics.md).

### Available Metrics

Ignite provides metrics across multiple categories including JVM performance, storage operations, SQL execution, network activity, and cluster coordination. Each metric source contains specific measurements relevant to that subsystem.

For the complete metrics catalog, see [Available Metrics](available-metrics.md).

### System Views

Query metrics directly using SQL through system views. This provides programmatic access to metrics data without requiring external monitoring tools.

For system view details, see [Metrics System Views](metrics-system-views.md).

## Performance Considerations

Metric collection adds overhead to cluster operations. Impact varies by metric source:

- JVM metrics have minimal overhead
- Storage and SQL metrics have moderate overhead
- Fine-grained operation metrics may have higher overhead

Enable metric sources selectively based on monitoring requirements and acceptable performance impact.
