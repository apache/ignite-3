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

package org.apache.ignite.internal.tx;

import static java.lang.String.format;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.lang.IgniteTriFunction;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Retry scenarios in runInTransaction.
 */
public class ItRunInTransactionTest extends ClusterPerTestIntegrationTest {
    protected static final String TABLE_NAME = "TEST";

    protected static final String COLUMN_KEY = "ID";

    protected static final String COLUMN_VAL = "VAl";

    @Override
    protected int initialNodes() {
        return 1;
    }

    Ignite ignite() {
        return node(0);
    }

    UUID txId(Transaction tx) {
        return ((InternalTransaction) tx).id();
    }

    private Table createTestTable() {
        ignite().sql().executeScript(
                "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" + COLUMN_KEY + " INT PRIMARY KEY, " + COLUMN_VAL + " VARCHAR)");

        return ignite().tables().table(TABLE_NAME);
    }

    void initAwareness(Table table) {
        table.partitionDistribution().primaryReplicasAsync().join();
    }

    @ParameterizedTest
    @MethodSource("syncTestContextFactory")
    public void testSync(SyncTestContext ctx) {
        Table table = createTestTable();
        initAwareness(table); // Makes test behavior determined.

        Transaction olderTx = ignite().transactions().begin();

        Tuple key = key(1);
        Tuple key2 = key(2);

        ctx.put.apply(ignite(), olderTx, key);

        AtomicInteger cnt = new AtomicInteger();

        CompletableFuture<Void> fut = IgniteTestUtils.runAsync(() -> {
            ignite().transactions().runInTransaction(youngerTx -> {
                if (cnt.incrementAndGet() == 2) {
                    throw new RuntimeException("retry");
                }

                ctx.put.apply(ignite(), youngerTx, key2);
                assertTrue(txId(olderTx).compareTo(txId(youngerTx)) < 0);
                // Younger is not allowed to wait for older.
                ctx.put.apply(ignite(), youngerTx, key);
            });
        });

        assertThat(fut, willThrowWithCauseOrSuppressed(Exception.class, "retry"));

        assertEquals(2, cnt.get(), "Should retry at least once");
    }

    @ParameterizedTest
    @MethodSource("asyncTestContextFactory")
    public void testAsync(AsyncTestContext ctx) {
        Table table = createTestTable();
        initAwareness(table); // Makes test behavior determined.

        Transaction olderTx = ignite().transactions().begin();

        Tuple key = key(1);
        Tuple key2 = key(2);

        ctx.put.apply(ignite(), olderTx, key).join();

        AtomicInteger cnt = new AtomicInteger();

        CompletableFuture<Void> fut = ignite().transactions().runInTransactionAsync(youngerTx -> {
            if (cnt.incrementAndGet() == 2) {
                throw new RuntimeException("retry");
            }

            return ctx.put.apply(ignite(), youngerTx, key2).thenCompose(r -> {
                assertTrue(txId(olderTx).compareTo(txId(youngerTx)) < 0,
                        "Wrong ordering: old=" + olderTx.toString() + ", new=" + youngerTx.toString());
                // Younger is not allowed to wait for older.
                return ctx.put.apply(ignite(), youngerTx, key);
            });
        });

        assertThat(fut, willThrowWithCauseOrSuppressed(Exception.class, "retry"));

        assertEquals(2, cnt.get(), "Should retry at least once");
    }

    protected static Tuple val(String v) {
        return Tuple.create().set(COLUMN_VAL, v);
    }

    protected static Tuple key(Integer k) {
        return Tuple.create().set(COLUMN_KEY, k);
    }

    private static Stream<Arguments> syncTestContextFactory() {
        return Stream.of(
                argumentSet("kv", new SyncTestContext(ItRunInTransactionTest::putKv)),
                argumentSet("sql", new SyncTestContext(ItRunInTransactionTest::putSql))
        );
    }

    private static Stream<Arguments> asyncTestContextFactory() {
        return Stream.of(
                argumentSet("kv", new AsyncTestContext(ItRunInTransactionTest::putKvAsync)),
                argumentSet("sql", new AsyncTestContext(ItRunInTransactionTest::putSqlAsync))
        );
    }

    /**
     * Sync test context.
     */
    protected static class SyncTestContext {
        final IgniteTriFunction<Ignite, Transaction, Tuple, Void> put;

        SyncTestContext(IgniteTriFunction<Ignite, Transaction, Tuple, Void> put) {
            this.put = put;
        }
    }

    /**
     * Async test context.
     */
    protected static class AsyncTestContext {
        final IgniteTriFunction<Ignite, Transaction, Tuple, CompletableFuture<Void>> put;

        AsyncTestContext(IgniteTriFunction<Ignite, Transaction, Tuple, CompletableFuture<Void>> put) {
            this.put = put;
        }
    }

    private static CompletableFuture<Void> putSqlAsync(Ignite client, Transaction tx, Tuple key) {
        return client.sql()
                .executeAsync(tx, format("INSERT INTO %s (%s, %s) VALUES (?, ?)", TABLE_NAME, COLUMN_KEY, COLUMN_VAL), key.intValue(0),
                        key.intValue(0) + "").thenApply(r -> null);
    }

    private static Void putKv(Ignite client, Transaction tx, Tuple key) {
        client.tables().tables().get(0).keyValueView().put(tx, key, val(key.intValue(0) + ""));
        return null;
    }

    private static Void putSql(Ignite client, @Nullable Transaction tx, Tuple key) {
        client.sql()
                .execute(tx, format("INSERT INTO %s (%s, %s) VALUES (?, ?)", TABLE_NAME, COLUMN_KEY, COLUMN_VAL), key.intValue(0),
                        key.intValue(0) + "");
        return null;
    }

    private static CompletableFuture<Void> putKvAsync(Ignite client, Transaction tx, Tuple key) {
        return client.tables().tables().get(0).keyValueView().putAsync(tx, key, val(key.intValue(0) + ""));
    }
}
