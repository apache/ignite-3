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

import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.table.TableRow.tuple;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.partition.HashPartition;
import org.apache.ignite.table.partition.PartitionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ItPartitionManagerTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "tableName";

    private static final int PARTITIONS = 3;

    @BeforeEach
    public void setup() {
        String zoneSql = "create zone test_zone with partitions=" + PARTITIONS + ", replicas=3, storage_profiles='" + DEFAULT_STORAGE_PROFILE + "'";
        String sql = "create table " + TABLE_NAME + " (key int primary key, val varchar(20)) with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });


        for (int i = 0; i < 1000; i++) {
            executeSql("INSERT INTO " + TABLE_NAME + " (key, val) VALUES (" + i + ", 'one')");
        }
    }

    @Test
    public void partitionsForAllKeys() {
        PartitionManager<HashPartition> partitionManager = cluster.aliveNode().tables().table(TABLE_NAME).partitionManager();
        TableViewInternal tableViewInternal = unwrapTableViewInternal(cluster.aliveNode().tables().table(TABLE_NAME));
        InternalTable internalTable = tableViewInternal.internalTable();

        CompletableFuture<?>[] futures = new CompletableFuture<?>[PARTITIONS];
        for (int i = 0; i < PARTITIONS; i++) {
            CompletableFuture<Object> future = new CompletableFuture<>();

            futures[i] = future;

            Publisher<BinaryRow> scan = internalTable.scan(i, null);

            HashPartition value = new HashPartition(i);

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
                    assertThat(partitionManager.partitionFromKeyAsync(key), willBe(value));
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

        assertThat(CompletableFuture.allOf(futures), willCompleteSuccessfully());
    }
}
