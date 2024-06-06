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

import static org.apache.ignite.internal.IndexTestUtils.waitForIndexToAppearInAnyState;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.partition.replica.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Testing RW transactions and indexes. */
@SuppressWarnings("resource")
public class ItRwTransactionAndIndexesTest extends ClusterPerClassIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String INDEX_NAME = "TEST_INDEX";

    private static final String PK_INDEX_NAME = pkIndexName(TABLE_NAME);

    private static final String COLUMN_NAME = "SALARY";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);

        CLUSTER.runningNodes().forEach(IgniteImpl::stopDroppingMessages);
    }

    private static IndexStorage indexStorage(TableImpl table, String indexName) {
        IndexStorage indexStorage = indexStorageOrNull(table, indexName);

        assertNotNull(indexStorage, indexName);

        return indexStorage;
    }

    @Test
    void testDropIndexInsideRwTransaction() {
        TableImpl table = unwrapTableImpl(createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1, DEFAULT_TEST_PROFILE_NAME));

        createIndex(TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        Transaction rwTx = beginRwTransaction();

        IndexStorage pkIndexStorage = indexStorage(table, PK_INDEX_NAME);
        IndexStorage droppedIndexStorage = indexStorage(table, INDEX_NAME);

        clearInvocations(pkIndexStorage, droppedIndexStorage);

        dropIndex(INDEX_NAME);

        insertPeopleInTransaction(rwTx, TABLE_NAME, newPerson(0));

        verify(pkIndexStorage).put(any());
        verify(droppedIndexStorage).put(any());

        assertDoesNotThrow(rwTx::commit);
    }

    private static IgniteImpl node() {
        return CLUSTER.node(0);
    }

    private static void dropAnyBuildIndexMessages() {
        node().dropMessages((s, networkMessage) -> networkMessage instanceof BuildIndexReplicaRequest);
    }

    private static Transaction beginRwTransaction() {
        Transaction tx = node().transactions().begin();

        assertFalse(tx.isReadOnly());

        return tx;
    }

    private static Person newPerson(int id) {
        return new Person(id, "person" + id, 100.0 + id);
    }

    @Nullable
    private static IndexStorage indexStorageOrNull(TableImpl table, String indexName) {
        return table.internalTable().storage().getIndex(0, indexId(indexName));
    }

    @Test
    void testCreateIndexInsideRwTransaction() throws Exception {
        TableImpl table = unwrapTableImpl(createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1, DEFAULT_TEST_PROFILE_NAME));

        dropAnyBuildIndexMessages();

        Transaction rwTx = beginRwTransaction();

        runAsync(() -> createIndex(TABLE_NAME, INDEX_NAME, COLUMN_NAME));

        waitForIndexToAppearInAnyState(INDEX_NAME, node());
        assertTrue(waitForCondition(() -> indexStorageOrNull(table, INDEX_NAME) != null, 10_000));

        IndexStorage pkIndexStorage = indexStorage(table, PK_INDEX_NAME);
        IndexStorage newIndexStorage = indexStorage(table, INDEX_NAME);

        clearInvocations(pkIndexStorage, newIndexStorage);

        insertPeopleInTransaction(rwTx, TABLE_NAME, newPerson(1));

        verify(pkIndexStorage).put(any());
        verify(newIndexStorage, never()).put(any());

        assertDoesNotThrow(rwTx::commit);
    }

    private static int indexId(String indexName) {
        IgniteImpl node = node();

        return TableTestUtils.getIndexIdStrict(node.catalogManager(), indexName, node.clock().nowLong());
    }
}
