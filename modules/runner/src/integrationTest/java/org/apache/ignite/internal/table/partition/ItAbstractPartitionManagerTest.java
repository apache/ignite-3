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

package org.apache.ignite.internal.table.partition;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.table.TableRow.tuple;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link PartitionManager}.
 */
public abstract class ItAbstractPartitionManagerTest extends ClusterPerTestIntegrationTest {
    protected static final String TABLE_NAME = "tableName";

    protected static final String ZONE_NAME = "TEST_ZONE";

    private static final int PARTITIONS = 3;

    protected abstract PartitionManager partitionManager();

    @BeforeEach
    public void setup() {
        String zoneSql = "create zone " + ZONE_NAME
                + " (partitions " + PARTITIONS + ","
                + " replicas 3)"
                + " storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";

        String sql = "create table " + TABLE_NAME + " (key int primary key, val varchar(20)) zone " + ZONE_NAME;

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });

        for (int i = 0; i < 1000; i++) {
            executeSql("INSERT INTO " + TABLE_NAME + " (key, val) VALUES (" + i + ", 'one')");
        }
    }

    @Test
    public void primaryPartitions() {
        TableViewInternal tableViewInternal = unwrapTableViewInternal(cluster.aliveNode().tables().table(TABLE_NAME));

        verifyPrimaryPartition(tableViewInternal, PARTITIONS);

        executeSql("ALTER TABLE " + TABLE_NAME + " ADD COLUMN val1 VARCHAR DEFAULT 'newDefault'");
        verifyPrimaryPartition(tableViewInternal, PARTITIONS);
        verifyAllKeys(tableViewInternal, PARTITIONS);
    }

    @Test
    public void partitionsForAllKeys() {
        TableViewInternal tableViewInternal = unwrapTableViewInternal(cluster.aliveNode().tables().table(TABLE_NAME));

        verifyAllKeys(tableViewInternal, PARTITIONS);
    }

    private void verifyPrimaryPartition(TableViewInternal tableViewInternal, int partitions) {
        InternalTable internalTable = tableViewInternal.internalTable();

        for (int i = 0; i < partitions; i++) {
            CompletableFuture<InternalClusterNode> clusterNodeCompletableFuture = internalTable.partitionLocation(i);

            CompletableFuture<ClusterNode> clusterNodeCompletableFuture1 = partitionManager()
                    .primaryReplicaAsync(new HashPartition(i));

            assertThat(clusterNodeCompletableFuture.join().id(), equalTo(clusterNodeCompletableFuture1.join().id()));
        }
    }

    private void verifyAllKeys(TableViewInternal tableViewInternal, int partitions) {
        InternalTable internalTable = tableViewInternal.internalTable();

        CompletableFuture<?>[] futures = new CompletableFuture<?>[partitions];
        for (int i = 0; i < partitions; i++) {
            CompletableFuture<Object> future = new CompletableFuture<>();

            futures[i] = future;

            Publisher<BinaryRow> scan = internalTable.scan(i, null);

            Partition value = new HashPartition(i);

            scan.subscribe(new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(BinaryRow item) {
                    SchemaRegistry registry = tableViewInternal.schemaView();
                    Tuple tuple = tuple(registry.resolve(item, registry.lastKnownSchemaVersion()));

                    Tuple key = Tuple.create().set("key", tuple.intValue("key"));
                    assertThat(partitionManager().partitionAsync(key), willBe(value));
                }

                @Override
                public void onError(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }

                @Override
                public void onComplete() {
                    future.complete(null);
                }
            });
        }

        assertThat(allOf(futures), willCompleteSuccessfully());
    }
}
