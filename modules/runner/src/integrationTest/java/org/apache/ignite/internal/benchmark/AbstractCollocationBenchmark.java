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

package org.apache.ignite.internal.benchmark;

import static org.apache.ignite.catalog.definitions.ColumnDefinition.column;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.PRODUCTION_CLUSTER_CONFIG_STRING;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

/**
 * Base class that allows to measure basic KeyValue operations for tables that share the same distribution zone.
 * TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this benchmark.
 */
public class AbstractCollocationBenchmark extends AbstractMultiNodeBenchmark {
    /** Name of shared zone. */
    private static final String SHARED_ZONE_NAME = "shared_zone";

    /** System property that allows to enable/disable collocation feature. */
    private static final String FEATURE_FLAG_NAME = "IGNITE_ZONE_BASED_REPLICATION";

    /** {@code true} enables collocation feature. */
    private static final Boolean ENABLE_COLLOCATION_FEATURE = true;

    protected final List<KeyValueView<Tuple, Tuple>> tableViews = new ArrayList<>();

    @Override
    protected int nodes() {
        return 1;
    }

    @Override
    protected int replicaCount() {
        return 1;
    }

    @Override
    protected String clusterConfiguration() {
        if (tinySchemaSyncWaits()) {
            return super.clusterConfiguration();
        } else {
            // Return a magic string that explicitly requests production defaults.
            return PRODUCTION_CLUSTER_CONFIG_STRING;
        }
    }

    @Override
    protected void createDistributionZoneOnStartup() {
        ZoneDefinition zone = ZoneDefinition.builder(SHARED_ZONE_NAME)
                .partitions(partitionCount())
                .replicas(replicaCount())
                .storageProfiles(DEFAULT_STORAGE_PROFILE)
                .build();

        publicIgnite.catalog().createZone(zone);
    }

    @Override
    protected void createTableOnStartup() {
        for (int i = 1; i <= tableCount(); ++i) {
            TableDefinition tableDefinition = TableDefinition.builder("test_table_" + i)
                    .columns(
                            column("id", ColumnType.INTEGER),
                            column("company", ColumnType.varchar(32)))
                    .primaryKey("id")
                    .zone(SHARED_ZONE_NAME)
                    .build();

            Table t = publicIgnite.catalog().createTable(tableDefinition);

            tableViews.add(t.keyValueView());
        }
    }

    @Override
    public void nodeSetUp() throws Exception {
        // Enable/disable collocation feature.
        System.setProperty(FEATURE_FLAG_NAME, ENABLE_COLLOCATION_FEATURE.toString());

        // Start the cluster and initialize it.
        super.nodeSetUp();

        // Patch replica manager to propagate table replication messages to zone replication groups.
        if (ENABLE_COLLOCATION_FEATURE) {
            int catalogVersion = igniteImpl
                    .catalogManager()
                    .latestCatalogVersion();

            int zoneId = igniteImpl
                    .catalogManager()
                    .catalog(catalogVersion)
                    .zone(SHARED_ZONE_NAME.toUpperCase())
                    .id();

            igniteImpl.replicaManager().groupIdConverter(request -> {
                if (!(request instanceof WriteIntentSwitchReplicaRequest)) {
                    if (request.groupId().asReplicationGroupId() instanceof TablePartitionId) {
                        TablePartitionId tablePartitionId = (TablePartitionId) request.groupId().asReplicationGroupId();

                        return new ZonePartitionId(zoneId, tablePartitionId.partitionId());
                    }
                }

                return request.groupId().asReplicationGroupId();
            });
        }
    }

    protected int tableCount() {
        return 1;
    }

    protected boolean tinySchemaSyncWaits() {
        return true;
    }
}
