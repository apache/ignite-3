package org.apache.ignite.internal.metrics.exporters;

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterConfigurationSchema;

/**
 * Empty configuration for {@link TestSimpleExporter}
 */
@PolymorphicConfigInstance(TestSimpleExporter.EXPORTER_NAME)
class TestSimpleExporterConfigurationSchema extends ExporterConfigurationSchema {
}
