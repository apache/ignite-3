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

package org.apache.ignite.internal.threading;

import static java.lang.Thread.currentThread;
import static org.apache.ignite.internal.PublicApiThreadingTests.anIgniteThread;
import static org.apache.ignite.internal.PublicApiThreadingTests.asyncContinuationPool;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.PartitionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ItPartitionManagerApiThreadingTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test";

    private static final int KEY = 1;

    private static final Tuple KEY_TUPLE = Tuple.create().set("id", 1);

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void createTable() {
        sql("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR)");
    }

    @AfterEach
    void dropTable() {
        sql("DROP TABLE " + TABLE_NAME);
    }

    @ParameterizedTest
    @EnumSource(PartitionManagerAsyncOperation.class)
    void partitionManagerFuturesCompleteInContinuationsPool(PartitionManagerAsyncOperation operation) {
        PartitionManager partitionManager = partitionManager();

        CompletableFuture<Thread> completerFuture = operation.executeOn(partitionManager)
                .thenApply(unused -> currentThread());

        assertThat(completerFuture, willBe(either(is(currentThread())).or(asyncContinuationPool())));
    }

    private static PartitionManager partitionManager() {
        return testTable().partitionManager();
    }

    private static Table testTable() {
        return CLUSTER.aliveNode().tables().table(TABLE_NAME);
    }

    @ParameterizedTest
    @EnumSource(PartitionManagerAsyncOperation.class)
    void partitionManagerFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(PartitionManagerAsyncOperation operation) {
        PartitionManager partitionManager = partitionManagerForInternalUse();

        CompletableFuture<Thread> completerFuture = operation.executeOn(partitionManager)
                .thenApply(unused -> currentThread());

        assertThat(completerFuture, willBe(either(is(currentThread())).or(anIgniteThread())));
    }

    private static PartitionManager partitionManagerForInternalUse() {
        return testTableForInternalUse().partitionManager();
    }

    private static Table testTableForInternalUse() {
        TableManager internalIgniteTables = unwrapTableManager(CLUSTER.aliveNode().tables());
        return internalIgniteTables.table(TABLE_NAME);
    }

    private enum PartitionManagerAsyncOperation {
        PRIMARY_REPLICA_ASYNC(distribution -> distribution.primaryReplicaAsync(new HashPartition(0))),
        PRIMARY_REPLICAS_ASYNC(distribution -> distribution.primaryReplicasAsync()),
        PARTITION_BY_TUPLE_ASYNC(distribution -> distribution.partitionAsync(KEY_TUPLE)),
        PARTITION_BY_KEY_ASYNC(distribution -> distribution.partitionAsync(KEY, Mapper.of(Integer.class)));

        private final Function<PartitionManager, CompletableFuture<?>> action;

        PartitionManagerAsyncOperation(Function<PartitionManager, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(PartitionManager partitionManager) {
            return action.apply(partitionManager);
        }
    }
}
