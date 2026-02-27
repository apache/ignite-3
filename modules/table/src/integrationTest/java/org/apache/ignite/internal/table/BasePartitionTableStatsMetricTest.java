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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Objects;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.table.distributed.PartitionTableStatsMetricSource;
import org.apache.ignite.table.QualifiedName;

/**
 * Base class for partition stats metrics tests.
 */
@SuppressWarnings("WeakerAccess")
abstract class BasePartitionTableStatsMetricTest extends BaseSqlIntegrationTest {

    static final int UNDEFINED_METRIC_VALUE = -1;

    static long metricFromAnyNode(String tableName, int partId, String metricName) {
        TableTuple table = TableTuple.of(tableName);

        return metricFromAnyNode(table.id, partId, metricName);
    }

    static long metricFromAnyNode(int tableId, int partId, String metricName) {
        for (int i = 0; i < CLUSTER.nodes().size(); i++) {
            long value = metricFromNode(i, tableId, partId, metricName);

            if (value != UNDEFINED_METRIC_VALUE) {
                return value;
            }
        }

        return UNDEFINED_METRIC_VALUE;
    }

    static long metricFromNode(int nodeIdx, int tableId, int partId, String metricName) {
        String metricSourceName =
                PartitionTableStatsMetricSource.formatSourceName(tableId, partId);

        MetricManager metricManager = unwrapIgniteImpl(node(nodeIdx)).metricManager();

        MetricSet metrics = metricManager.metricSnapshot().metrics().get(metricSourceName);

        if (metrics != null) {
            LongMetric metric = metrics.get(metricName);
            Objects.requireNonNull(metric, "metric does not exist: " + metricName);

            return metric.value();
        }

        return UNDEFINED_METRIC_VALUE;
    }

    static void sqlScript(String... queries) {
        CLUSTER.aliveNode().sql().executeScript(String.join(";", queries));
    }

    static void enableStats(String tableName) {
        TableTuple table = TableTuple.of(tableName);

        for (int p = 0; p < table.partsCount; p++) {
            String metricName = PartitionTableStatsMetricSource.formatSourceName(table.id, p);

            for (int i = 0; i < CLUSTER.nodes().size(); i++) {
                enableMetricSource(metricName, i);
            }
        }
    }

    static void enableMetricSource(String sourceName, int nodeIdx) {
        IgniteImpl node = unwrapIgniteImpl(node(nodeIdx));
        node.metricManager().metricSources().stream()
                .filter(ms -> sourceName.equals(ms.name()))
                .findAny()
                .ifPresent(ms -> node.metricManager().enable(ms));
    }

    static final class TableTuple {
        final int id;
        final int partsCount;

        TableTuple(int id, int partsCount) {
            this.id = id;
            this.partsCount = partsCount;
        }

        static TableTuple of(String tableName) {
            QualifiedName qualifiedName = QualifiedName.parse(tableName);
            Catalog catalog = unwrapIgniteImpl(node(0)).catalogManager().latestCatalog();

            CatalogTableDescriptor tableDesc = catalog.table(qualifiedName.schemaName(), qualifiedName.objectName());
            assertNotNull(tableDesc);

            CatalogZoneDescriptor zoneDesc = catalog.zone(tableDesc.zoneId());
            assertNotNull(zoneDesc);

            int partsCount = zoneDesc.partitions();

            return new TableTuple(tableDesc.id(), partsCount);
        }
    }
}
