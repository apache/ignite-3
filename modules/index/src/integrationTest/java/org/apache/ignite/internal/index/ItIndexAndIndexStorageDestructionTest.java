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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.lang.ErrorGroups.Storage.ALREADY_DESTROYED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.AbstractPageMemoryIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.index.AbstractRocksDbIndexStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.CriteriaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests about accessing destroyed index storages.
 */
class ItIndexAndIndexStorageDestructionTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";
    private static final String INDEX_NAME = "TEST_INDEX";

    private static final int PREEXISTING_KEY = 1;
    private static final int ANOTHER_KEY = 2;

    private Ignite node;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void createTableAndIndex() {
        node = cluster.aliveNode();

        cluster.doInSession(0, session -> {
            session.execute(null, "CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name VARCHAR)");
            session.execute(null, "CREATE INDEX " + INDEX_NAME + " ON " + TABLE_NAME + "(name)");

            session.execute(null, "INSERT INTO " + TABLE_NAME + " (id, name) VALUES (" + PREEXISTING_KEY + ", 'John')");
        });

        TableImpl table = unwrapTableImpl(node.tables().table(TABLE_NAME));

        initiateIndexStoragesDestruction(table, INDEX_NAME);
    }

    private void initiateIndexStoragesDestruction(TableImpl table, String indexName) {
        int indexId = indexId(indexName);

        for (int partitionNumber = 0; partitionNumber < table.internalTable().partitions(); partitionNumber++) {
            TableIndexStoragesSupplier storagesSupplier = table.indexStorageAdapters(partitionNumber);
            TableSchemaAwareIndexStorage schemaAwareStorage = storagesSupplier.get().get(indexId);
            IndexStorage indexStorage = schemaAwareStorage.storage();

            if (indexStorage instanceof AbstractPageMemoryIndexStorage) {
                ((AbstractPageMemoryIndexStorage<?, ?, ?>) indexStorage).transitionToDestroyedState();
            } else if (indexStorage instanceof AbstractRocksDbIndexStorage) {
                ((AbstractRocksDbIndexStorage) indexStorage).transitionToDestroyedState();
            } else {
                fail("Index storage is not of a type supported by the test: " + indexStorage.getClass());
            }
        }
    }

    private int indexId(String indexName) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node);
        CatalogIndexDescriptor indexDescriptor = igniteImpl.catalogManager().aliveIndex(indexName, igniteImpl.clock().nowLong());
        assertThat(indexDescriptor, is(notNullValue()));

        return indexDescriptor.id();
    }

    @Test
    void insertTouchingDestroyedIndexStorageDoesNotFailWholeWrite() {
        KeyValueView<Integer, String> view = node.tables().table(TABLE_NAME)
                .keyValueView(Integer.class, String.class);

        assertDoesNotThrow(() -> view.put(null, ANOTHER_KEY, "Mary"));

        assertThat(view.get(null, ANOTHER_KEY), is("Mary"));
    }

    @Test
    void updateTouchingDestroyedIndexStorageDoesNotFailWholeWrite() {
        KeyValueView<Integer, String> view = node.tables().table(TABLE_NAME)
                .keyValueView(Integer.class, String.class);

        assertDoesNotThrow(() -> view.put(null, PREEXISTING_KEY, "Mary"));

        assertThat(view.get(null, PREEXISTING_KEY), is("Mary"));
    }

    @Test
    void replaceTouchingDestroyedIndexStorageDoesNotFailWholeWrite() {
        KeyValueView<Integer, String> view = node.tables().table(TABLE_NAME)
                .keyValueView(Integer.class, String.class);

        assertDoesNotThrow(() -> view.replace(null, PREEXISTING_KEY, "John", "Mary"));

        assertThat(view.get(null, PREEXISTING_KEY), is("Mary"));
    }

    @Test
    void sqlReadFromDestroyedIndexStorageFailsWithStalePlanError() {
        SqlException ex = assertThrows(
                SqlException.class,
                () -> cluster.query(0, "SELECT * FROM " + TABLE_NAME + " WHERE name = 'John'", rs -> null)
        );

        assertThat(ex.code(), is(ALREADY_DESTROYED_ERR));
        assertThat(
                ex.getMessage(),
                startsWith("Read from an index storage that is in the process of being destroyed or already destroyed")
        );
    }

    @Test
    void kvQueryFromDestroyedIndexStorageFailsWithStalePlanError() {
        KeyValueView<Tuple, Tuple> view = node.tables().table(TABLE_NAME).keyValueView();

        CriteriaException ex = assertThrows(CriteriaException.class, () -> {
            try (Cursor<?> cursor = view.query(null, null, INDEX_NAME)) {
                cursor.next();
            }
        });

        assertThat(ex.code(), is(ALREADY_DESTROYED_ERR));
        assertThat(
                ex.getMessage(),
                startsWith("Read from an index storage that is in the process of being destroyed or already destroyed")
        );
    }
}
