package org.apache.ignite.internal.metrics.exporters;

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterConfigurationSchema;

@PolymorphicConfigInstance(TestSimpleExporter.EXPORTER_NAME)
public class TestSimpleExporterConfigurationSchema extends ExporterConfigurationSchema {
}
