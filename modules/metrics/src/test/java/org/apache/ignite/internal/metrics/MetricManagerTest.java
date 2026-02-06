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

import static org.apache.ignite.internal.metrics.exporters.jmx.JmxExporter.JMX_EXPORTER_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.IgniteUtils.makeMbeanName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.lang.management.ManagementFactory;
import java.util.UUID;
import javax.management.ObjectName;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.sources.OsMetricSource;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@link MetricManager}.
 */
@ExtendWith(ConfigurationExtension.class)
public class MetricManagerTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(MetricManagerTest.class);

    private static final String NODE_NAME = "test-node-name";

    private static final UUID NODE_ID = UUID.randomUUID();

    @InjectConfiguration("mock.exporters = {" + JMX_EXPORTER_NAME + " = {exporterName = " + JMX_EXPORTER_NAME + "}}")
    private MetricConfiguration jmxMetricConfiguration;

    @Test
    void metricSourceRegistration() throws Exception {
        MetricManager metricManager = createAndStartManager(jmxMetricConfiguration);

        OsMetricSource source = new OsMetricSource();
        metricManager.registerSource(source);

        MetricSet metricSet = metricManager.enable(source);
        assertThat(metricSet, notNullValue());

        ObjectName name = makeMbeanName(NODE_NAME, metricSet.group(), metricSet.name());
        assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(name), is(true));

        stopManager(metricManager);

        assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(name), is(false));
    }

    @Test
    void metricSourceCleanup() throws Exception {
        MetricManager metricManager = createAndStartManager(jmxMetricConfiguration);

        OsMetricSource source = new OsMetricSource();
        metricManager.registerSource(source);

        MetricSet metricSet = metricManager.enable(source);
        assertThat(metricSet, notNullValue());

        ObjectName name = makeMbeanName(NODE_NAME, metricSet.group(), metricSet.name());
        assertThat(
                "Metric source was not registered.",
                ManagementFactory.getPlatformMBeanServer().isRegistered(name),
                is(true));

        metricManager.unregisterSource(source);

        assertThat(
                "Metric source was not disabled before unregistering.",
                ManagementFactory.getPlatformMBeanServer().isRegistered(name),
                is(false));

        stopManager(metricManager);
    }

    private static MetricManager createAndStartManager(MetricConfiguration configuration) {
        MetricManager metricManager = new MetricManagerImpl(LOG, null);

        metricManager.configure(configuration, () -> NODE_ID, NODE_NAME);

        assertThat(metricManager.startAsync(new ComponentContext()), willSucceedFast());

        return metricManager;
    }

    private static void stopManager(MetricManager metricManager) {
        metricManager.beforeNodeStop();
        assertThat(metricManager.stopAsync(new ComponentContext()), willSucceedFast());
    }
}
