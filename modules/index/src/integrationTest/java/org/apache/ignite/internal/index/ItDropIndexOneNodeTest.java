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

package org.apache.ignite.internal.index;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for scenarios related to dropping of indices, executed on a single node cluster.
 */
@SuppressWarnings({"resource", "ThrowableNotThrown"})
public class ItDropIndexOneNodeTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "TEST";

    private static final String INDEX_NAME = "TEST_IDX";

    private static final String COLUMN_NAME = "name";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void createTable() {
        createTable(TABLE_NAME, 1, 1);

        createIndex(TABLE_NAME, INDEX_NAME, COLUMN_NAME);
    }

    @AfterEach
    void cleanup() {
        dropAllTables();
    }

    @Test
    void testCreateIndexAfterDrop() {
        dropIndex(INDEX_NAME);

        assertDoesNotThrow(() -> createIndex(TABLE_NAME, INDEX_NAME, COLUMN_NAME));
    }

    @Test
    void testCreateIndexAfterDropWhileTransactionInProgress() {
        Transaction tx = CLUSTER.aliveNode().transactions().begin();

        dropIndex(INDEX_NAME);

        CompletableFuture<Void> creationFuture = runAsync(() -> createIndex(TABLE_NAME, INDEX_NAME, COLUMN_NAME));

        tx.commit();

        assertThat(creationFuture, willCompleteSuccessfully());
    }

    @Test
    void testDoubleCreateIndex() {
        assertThrowsWithCause(() -> createIndex(TABLE_NAME, INDEX_NAME, COLUMN_NAME), CatalogValidationException.class);
    }

    @Test
    void testDropIndexColumn() {
        dropIndex(INDEX_NAME);

        assertDoesNotThrow(ItDropIndexOneNodeTest::dropIndexedColumn);
    }

    @Test
    void testDropIndexColumnWhileTransactionInProgress() {
        runInRwTransaction(tx -> {
            dropIndex(INDEX_NAME);

            assertDoesNotThrow(ItDropIndexOneNodeTest::dropIndexedColumn);
        });
    }

    @Test
    void testDropIndexColumnFails() {
        assertThrowsWithCause(ItDropIndexOneNodeTest::dropIndexedColumn, CatalogValidationException.class);
    }

    private static void runInRwTransaction(Consumer<Transaction> action) {
        CLUSTER.aliveNode().transactions().runInTransaction(action, new TransactionOptions().readOnly(false));
    }

    private static void dropIndexedColumn() {
        sql(String.format("ALTER TABLE %s DROP COLUMN %s", TABLE_NAME, COLUMN_NAME));
    }
}
