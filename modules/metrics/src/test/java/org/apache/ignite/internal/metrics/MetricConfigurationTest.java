/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.TestExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.TestExporterChange;
import org.apache.ignite.internal.metrics.exporters.configuration.TestExporterConfigurationSchema;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for exporters' configurations.
 */
@ExtendWith(ConfigurationExtension.class)
public class MetricConfigurationTest extends BaseIgniteAbstractTest {
    @InjectConfiguration(
            polymorphicExtensions = {
                    TestExporterConfigurationSchema.class
            }
    )
    private MetricConfiguration metricConfiguration;

    private MetricManagerImpl metricManager;

    private TestExporter exporter;

    @BeforeEach
    public void setUp() {
        metricManager = new MetricManagerImpl("test-node", UUID::randomUUID, metricConfiguration);

        exporter  = new TestExporter();

        metricManager.start(Map.of("test", exporter));
    }

    @Test
    public void testExporterStartStop() {
        assertFalse(exporter.isStarted());

        metricConfiguration.exporters().change(ch -> {
            ch.create("test", exporterChange -> {
                exporterChange.convert("test");
            });
        }).join();

        assertTrue(exporter.isStarted());

        metricConfiguration.exporters().change(ch -> {
            ch.delete("test");
        }).join();

        assertFalse(exporter.isStarted());
    }

    @Test
    public void testExporterReconfiguration() {
        metricConfiguration.exporters().change(ch -> {
            ch.create("test", exporterChange -> {
                exporterChange.convert("test");
            });
        }).join();

        assertEquals(0, exporter.port());

        metricConfiguration.exporters().change(ch -> {
            ch.update("test", exporterChange -> {
                ((TestExporterChange) exporterChange.convert("test")).changePort(32);
            });
        }).join();

        assertEquals(32, exporter.port());

        metricConfiguration.exporters().get("test").change(ch -> {
            ((TestExporterChange) ch).changePort(33);
        }).join();

        assertEquals(33, exporter.port());
    }
}
