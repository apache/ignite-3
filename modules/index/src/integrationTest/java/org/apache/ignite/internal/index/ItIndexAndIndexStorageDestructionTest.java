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

import static org.apache.ignite.lang.ErrorGroups.Sql.STALE_PLAN_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

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

    private IgniteImpl node;

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

            session.execute(null, "INSERT INTO " + TABLE_NAME + " (id, name) VALUES (1, 'John')");
        });

        TableImpl table = (TableImpl) node.tables().table(TABLE_NAME);

        initiateIndexStoragesDestruction(table, INDEX_NAME);
    }

    private void initiateIndexStoragesDestruction(TableImpl table, String indexName) {
        int indexId = indexId(indexName);

        for (int partitionNumber = 0; partitionNumber < table.internalTable().partitions(); partitionNumber++) {
            TableIndexStoragesSupplier storagesSupplier = table.indexStorageAdapters(partitionNumber);
            TableSchemaAwareIndexStorage schemaAwareStorage = storagesSupplier.get().get(indexId);
            IndexStorage indexStorage = schemaAwareStorage.storage();

            if (indexStorage instanceof AbstractPageMemoryIndexStorage) {
                ((AbstractPageMemoryIndexStorage<?, ?>) indexStorage).transitionToDestroyedState();
            } else if (indexStorage instanceof AbstractRocksDbIndexStorage) {
                ((AbstractRocksDbIndexStorage) indexStorage).transitionToDestroyedState();
            } else {
                fail("Index storage is not of a type supported by the test: " + indexStorage.getClass());
            }
        }
    }

    private int indexId(String indexName) {
        CatalogIndexDescriptor indexDescriptor = node.catalogManager().aliveIndex(indexName, node.clock().nowLong());
        assertThat(indexDescriptor, is(notNullValue()));

        return indexDescriptor.id();
    }

    @Test
    void writeToDestroyedIndexStorageDoesNotFailWholeWrite() {
        KeyValueView<Integer, String> view = node.tables().table(TABLE_NAME)
                .keyValueView(Integer.class, String.class);

        assertDoesNotThrow(() -> view.put(null, 2, "Mary"));

        assertThat(view.get(null, 2), is("Mary"));
    }

    @Test
    void sqlReadFromDestroyedIndexStorageFailsWithStalePlanError() {
        SqlException ex = assertThrows(
                SqlException.class,
                () -> cluster.query(0, "SELECT * FROM " + TABLE_NAME + " WHERE name = 'John'", rs -> null)
        );

        assertThat(ex.code(), is(STALE_PLAN_ERR));
        assertThat(ex.getMessage(), is("The plan is stale. Please retry."));
    }

    @Test
    void kvQueryFromDestroyedIndexStorageFailsWithStalePlanError() {
        KeyValueView<Tuple, Tuple> view = node.tables().table(TABLE_NAME).keyValueView();

        CriteriaException ex = assertThrows(CriteriaException.class, () -> {
            try (Cursor<?> cursor = view.query(null, null, INDEX_NAME)) {
                cursor.next();
            }
        });

        assertThat(ex.code(), is(STALE_PLAN_ERR));
        assertThat(ex.getMessage(), is("The plan is stale. Please retry."));
    }
}
