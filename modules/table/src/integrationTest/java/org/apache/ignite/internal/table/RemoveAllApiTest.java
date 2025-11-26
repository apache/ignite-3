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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for validating the removal of all entries using the Key-Value API. Extends the ClusterPerClassIntegrationTest to reuse cluster
 * setup functionality.
 */
public class RemoveAllApiTest extends ClusterPerClassIntegrationTest {
    public static final String TABLE_NAME = "PERSON";

    /** Distributed table for testing. */
    private Table table;

    /** Ignite client used to interact with the Ignite cluster. */
    private IgniteClient client;

    @BeforeAll
    public void beforeAll() {
        table = createZoneAndTable("ZONE1", TABLE_NAME, 1, 32);
        client = IgniteClient.builder().addresses("localhost").build();
    }

    @BeforeEach
    public void beforeEach() {
        for (int i = 0; i < 100; i++) {
            insertPeople(TABLE_NAME, new Person(i, "Name " + i, i % 5 * 1000L));
        }
    }

    @AfterAll
    public void afterAll() {
        client.close();
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    private List<Runnable> removeAllOps() {
        return List.of(
                () -> table.keyValueView().removeAll(null),
                () -> table.keyValueView(Integer.class, Person.class).removeAll(null),
                () -> client.tables().table(TABLE_NAME).keyValueView().removeAll(null),
                () -> client.tables().table(TABLE_NAME).keyValueView(Integer.class, Person.class).removeAll(null),
                () -> table.recordView().deleteAll(null),
                () -> client.tables().table(TABLE_NAME).recordView().deleteAll(null),
                () -> table.recordView(Person.class).deleteAll(null),
                () -> client.tables().table(TABLE_NAME).recordView(Person.class).deleteAll(null)
        );
    }

    private List<Supplier<CompletableFuture<Void>>> removeAllAsyncOps() {
        return List.of(
                () -> table.keyValueView().removeAllAsync(null),
                () -> client.tables().table(TABLE_NAME).keyValueView().removeAllAsync(null),
                () -> table.recordView().deleteAllAsync(null),
                () -> client.tables().table(TABLE_NAME).recordView().deleteAllAsync(null),
                () -> table.recordView(Person.class).deleteAllAsync(null),
                () -> client.tables().table(TABLE_NAME).recordView(Person.class).deleteAllAsync(null)
        );
    }

    private List<Consumer<Transaction>> removeAllTxOps() {
        return List.of(
                tx -> table.keyValueView().removeAll(tx),
                tx -> table.keyValueView(Integer.class, Person.class).removeAll(tx),
                tx -> table.recordView().deleteAll(tx),
                tx -> table.recordView(Person.class).deleteAll(tx)
        );
    }

    private List<Consumer<Transaction>> removeAllClientTxOps() {
        return List.of(
                tx -> client.tables().table(TABLE_NAME).keyValueView().removeAll(tx),
                tx -> client.tables().table(TABLE_NAME).keyValueView(Integer.class, Person.class).removeAll(tx),
                tx -> client.tables().table(TABLE_NAME).recordView().deleteAll(tx),
                tx -> client.tables().table(TABLE_NAME).recordView(Person.class).deleteAll(tx)
        );
    }

    @ParameterizedTest
    @MethodSource("removeAllOps")
    void testRemoveAll(Runnable op) {
        checkSize(100L);

        op.run();

        checkSize(0L);
    }

    @ParameterizedTest
    @MethodSource("removeAllAsyncOps")
    void testRemoveAllAsync(Supplier<CompletableFuture<Void>> op) {
        checkSize(100L);

        assertThat(op.get(), willCompleteSuccessfully());

        checkSize(0L);
    }

    @ParameterizedTest
    @MethodSource("removeAllTxOps")
    void testRemoveAllInTx(Consumer<Transaction> op) {
        checkSize(100L);

        Transaction tx = node(0).transactions().begin();

        op.accept(tx);

        checkSize(100L);

        tx.commit();

        checkSize(0L);
    }

    @ParameterizedTest
    @MethodSource("removeAllClientTxOps")
    void testRemoveAllInClientTx(Consumer<Transaction> op) {
        checkSize(100L);

        Transaction tx = client.transactions().begin();

        op.accept(tx);

        checkSize(100L);

        tx.commit();

        checkSize(0L);
    }

    /**
     * Checks whether the table size is equal to the expected value.
     *
     * @param expected Expected table size.
     */
    private static void checkSize(long expected) {
        List<List<Object>> res = sql("SELECT COUNT(*) FROM " + TABLE_NAME);

        assertEquals(expected, res.get(0).get(0));
    }
}
