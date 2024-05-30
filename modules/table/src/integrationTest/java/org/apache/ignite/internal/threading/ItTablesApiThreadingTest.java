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

import static org.apache.ignite.internal.PublicApiThreadingTests.anIgniteThread;
import static org.apache.ignite.internal.PublicApiThreadingTests.asyncContinuationPool;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.PublicApiThreadingTests;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.table.IgniteTables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@SuppressWarnings("resource")
class ItTablesApiThreadingTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    void createTable() {
        sql("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR)");
    }

    @ParameterizedTest
    @EnumSource(TablesAsyncOperation.class)
    void futuresCompleteInContinuationsPool(TablesAsyncOperation operation) {
        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(CLUSTER.aliveNode().tables())
                        .thenApply(unused -> Thread.currentThread())
        );

        assertThat(completerFuture, willBe(asyncContinuationPool()));
    }

    @ParameterizedTest
    @EnumSource(TablesAsyncOperation.class)
    void futuresFromInternalCallsAreNotResubmittedToContinuationsPool(TablesAsyncOperation operation) {
        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(igniteTablesForInternalUse())
                        .thenApply(unused -> Thread.currentThread())
        );

        assertThat(completerFuture, willBe(anIgniteThread()));
    }

    private static <T> T forcingSwitchFromUserThread(Supplier<? extends T> action) {
        return PublicApiThreadingTests.tryToSwitchFromUserThreadWithDelayedSchemaSync(CLUSTER.aliveNode(), action);
    }

    private static TableManager igniteTablesForInternalUse() {
        return unwrapTableManager(CLUSTER.aliveNode().tables());
    }

    private enum TablesAsyncOperation {
        TABLE_ASYNC(tables -> tables.tableAsync(TABLE_NAME)),
        TABLES_ASYNC(tables -> tables.tablesAsync());

        private final Function<IgniteTables, CompletableFuture<?>> action;

        TablesAsyncOperation(Function<IgniteTables, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(IgniteTables igniteTables) {
            return action.apply(igniteTables);
        }
    }
}
