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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSource;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.table.QualifiedName;
import org.jetbrains.annotations.Nullable;

/**
 * Holder of the various metric sources for the tables.
 */
class TableMetrics {
    private static final IgniteLogger LOG = Loggers.forClass(TableMetrics.class);

    private final MetricManager metricManager;
    private final DataStorageManager dataStorageMgr;

    private final Map<TablePartitionId, PartitionModificationCounterMetricSource> partModCounterMetricSources = new ConcurrentHashMap<>();

    private final Map<Integer, TableMetricSource> tableMetricSources = new ConcurrentHashMap<>();
    private final Map<Integer, StorageEngineTablesMetricSource> engineMetricSources = new ConcurrentHashMap<>();

    TableMetrics(MetricManager metricManager, DataStorageManager dataStorageMgr) {
        this.metricManager = metricManager;
        this.dataStorageMgr = dataStorageMgr;
    }

    void registerPartitionModificationCounterMetrics(TableViewInternal table, int partitionId, PartitionModificationCounter counter) {
        var metricSource = new PartitionModificationCounterMetricSource(table.tableId(), partitionId);

        metricSource.addMetric(new LongGauge(
                PartitionModificationCounterMetricSource.METRIC_COUNTER,
                "The value of the volatile counter of partition modifications. "
                        + "This value is used to determine staleness of the related SQL statistics.",
                counter::value
        ));

        metricSource.addMetric(new LongGauge(
                PartitionModificationCounterMetricSource.METRIC_NEXT_MILESTONE,
                "The value of the next milestone for the number of partition modifications. "
                        + "This value is used to determine staleness of the related SQL statistics.",
                counter::nextMilestone
        ));

        metricSource.addMetric(new LongGauge(
                PartitionModificationCounterMetricSource.METRIC_LAST_MILESTONE_TIMESTAMP,
                "The timestamp value representing the commit time of the last modification operation that "
                        + "reached the milestone. This value is used to determine staleness of the related SQL statistics.",
                () -> counter.lastMilestoneTimestamp().longValue()
        ));

        try {
            metricManager.registerSource(metricSource);
            metricManager.enable(metricSource);

            partModCounterMetricSources.put(new TablePartitionId(table.tableId(), partitionId), metricSource);
        } catch (Exception e) {
            LOG.warn("Failed to register partition modification metrics source for table [name={}, partitionId={}].", e,
                    table.name(), partitionId);
        }
    }

    void unregisterPartitionModificationCounterMetricSource(TablePartitionId tablePartitionId, TableViewInternal table) {
        PartitionModificationCounterMetricSource metricSource = partModCounterMetricSources.remove(tablePartitionId);
        String message = "partition modification metrics source for table [name={}, partitionId={}].";
        unregisterMetricSource(
                metricSource,
                () -> format(message, table.name(), tablePartitionId.partitionId())
        );
    }

    TableMetricSource createAndRegisterMetricsSource(
            StorageTableDescriptor tableDescriptor,
            QualifiedName tableName,
            @Nullable TableMetricSource oldTableSource,
            @Nullable StorageEngineTablesMetricSource oldEngineSource
    ) {
        int tableId = tableDescriptor.getId();

        StorageEngine engine = dataStorageMgr.engineByStorageProfile(tableDescriptor.getStorageProfile());

        // Engine can be null sometimes, see "TableManager.createTableStorage".
        if (engine != null) {
            var engineMetricSource = new StorageEngineTablesMetricSource(engine.name(), tableName, oldEngineSource);

            engine.addTableMetrics(tableDescriptor, engineMetricSource);

            try {
                metricManager.registerSource(engineMetricSource);
                metricManager.enable(engineMetricSource);

                engineMetricSources.put(tableId, engineMetricSource);
            } catch (Exception e) {
                String message = "Failed to register storage engine metrics source for table [id={}, name={}].";
                LOG.warn(message, e, tableId, tableName);
            }
        }

        TableMetricSource source = new TableMetricSource(tableName, oldTableSource);

        try {
            metricManager.registerSource(source);
            metricManager.enable(source);

            tableMetricSources.put(tableId, source);
        } catch (Exception e) {
            LOG.warn("Failed to register metrics source for table [id={}, name={}].", e, tableId, tableName);
        }

        return source;
    }

    void unregisterMetricsSource(TableViewInternal table) {
        if (table == null) {
            return;
        }

        TableMetricSource tableMetricSource = tableMetricSources.remove(table.tableId());
        unregisterMetricSource(
                tableMetricSource,
                () -> format("metrics source for table [id={}, name={}]", table.tableId(), table.qualifiedName())
        );

        StorageEngineTablesMetricSource engineMetricSource = engineMetricSources.remove(table.tableId());
        unregisterMetricSource(
                engineMetricSource,
                () -> format("storage enging metrics source for table [id={}, name={}]", table.tableId(), table.qualifiedName())
        );
    }

    void renameMetricsSource(TableViewInternal table) {
        TableMetricSource tableMetricSource = tableMetricSources.get(table.tableId());
        StorageEngineTablesMetricSource engineMetricSource = engineMetricSources.get(table.tableId());

        unregisterMetricsSource(table);

        StorageTableDescriptor storageTableDescriptor = table.internalTable().storage().getTableDescriptor();
        createAndRegisterMetricsSource(storageTableDescriptor, table.qualifiedName(), tableMetricSource, engineMetricSource);
    }

    private void unregisterMetricSource(MetricSource metricSource, Supplier<String> messageSupplier) {
        if (metricSource != null) {
            try {
                metricManager.unregisterSource(metricSource);
            } catch (Exception e) {
                LOG.warn("Failed to unregister {}.", e, messageSupplier);
            }
        }
    }
}
