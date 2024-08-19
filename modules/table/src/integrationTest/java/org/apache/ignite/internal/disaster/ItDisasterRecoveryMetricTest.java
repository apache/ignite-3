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

package org.apache.ignite.internal.disaster;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.metrics.exporters.jmx.JmxExporter.JMX_METRIC_GROUP;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.getTableStrict;
import static org.apache.ignite.internal.util.IgniteUtils.makeMbeanName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.management.ManagementFactory;
import java.util.List;
import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.metrics.MetricSource;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** For integration testing of disaster recovery metrics. */
public class ItDisasterRecoveryMetricTest extends BaseSqlIntegrationTest {
    private static final String ZONE_NAME = "ZONE_" + TABLE_NAME;

    private final MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        super.configureInitParameters(builder);

        builder.clusterConfiguration("{metrics.exporters.jmx.exporterName: jmx}");
    }

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);
    }

    @Test
    void testPartitionStatesMetricsForTable() throws Exception {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, initialNodes(), 1);

        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        String expectedMetricSourceName = partitionStatesMetricSourceName(node, TABLE_NAME);

        // Let's make sure that the metric source is registered.
        assertThat(registeredMetricSourceNames(node), hasItem(expectedMetricSourceName));

        node.metricManager().enable(expectedMetricSourceName);

        DynamicMBean metricSourceMbean = metricSourceMbean(expectedMetricSourceName);

        assertEquals(0L, metricSourceMbean.getAttribute("UnavailablePartitionCount"));
        assertEquals(1L, metricSourceMbean.getAttribute("HealthyPartitionCount"));
        assertEquals(0L, metricSourceMbean.getAttribute("InitializingPartitionCount"));
        assertEquals(0L, metricSourceMbean.getAttribute("InstallingSnapshotPartitionCount"));
        assertEquals(0L, metricSourceMbean.getAttribute("BrokenPartitionCount"));
    }

    @Test
    void testPartitionStatesMetricsForTableAfterDropTable() {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, initialNodes(), 1);

        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());

        String expectedMetricSourceName = partitionStatesMetricSourceName(node, TABLE_NAME);

        sql("DROP TABLE IF EXISTS " + TABLE_NAME);

        // Let's make sure that the metric source is unregistered.
        assertThat(registeredMetricSourceNames(node), not(hasItem(expectedMetricSourceName)));
    }

    private DynamicMBean metricSourceMbean(String metricSourceName) throws Exception {
        ObjectName mbeanName = makeMbeanName(JMX_METRIC_GROUP, metricSourceName);

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, DynamicMBean.class, false);
    }

    private static String partitionStatesMetricSourceName(IgniteImpl node, String tableName) {
        CatalogTableDescriptor tableDescriptor = getTableStrict(node.catalogManager(), tableName, node.clock().nowLong());

        return String.format("partition.states.zone.%s.table.%s", tableDescriptor.zoneId(), tableDescriptor.id());
    }

    private static List<String> registeredMetricSourceNames(IgniteImpl node) {
        return node.metricManager().metricSources().stream().map(MetricSource::name).collect(toList());
    }
}
