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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.replication.request.BuildIndexReplicaRequest;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Testing RW transactions and indexes. */
public class ItRwTransactionAndIndexesTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String INDEX_NAME = "TEST_INDEX";

    private static final String PK_INDEX_NAME = pkIndexName(TABLE_NAME);

    private static final String COLUMN_NAME = "SALARY";

    private static final String ENGINE = "test";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + zoneName(TABLE_NAME));

        CLUSTER.runningNodes().forEach(IgniteImpl::stopDroppingMessages);
    }

    @Test
    void testCreateIndexInsideRwTransaction() {
        TableImpl table = createTable(TABLE_NAME, 1, 1, ENGINE);

        setAwaitIndexAvailability(false);
        dropBuildAllIndex();

        Transaction rwTx = beginRwTransaction();

        createIndex(TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        IndexStorage[] indexStorages = indexStorages(table, PK_INDEX_NAME, INDEX_NAME);
        clearInvocations(indexStorages);

        insertPeopleInTransaction(rwTx, TABLE_NAME, newPerson(1));

        verifyPutIntoIndexes(indexStorages);

        rwTx.commit();
    }

    @Test
    void testDropIndexInsideRwTransaction() {
        TableImpl table = createTable(TABLE_NAME, 1, 1, ENGINE);

        createIndex(TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        Transaction rwTx = beginRwTransaction();

        IndexStorage[] indexStorages = indexStorages(table, PK_INDEX_NAME, INDEX_NAME);
        clearInvocations(indexStorages);

        dropIndex(INDEX_NAME);

        insertPeopleInTransaction(rwTx, TABLE_NAME, newPerson(0));

        verifyPutIntoIndexes(indexStorages);

        rwTx.commit();
    }

    private static IgniteImpl node() {
        return CLUSTER.node(0);
    }

    private static void dropBuildAllIndex() {
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

    private static IndexStorage[] indexStorages(TableImpl table, String... indexNames) {
        IndexStorage[] indexStorages = new IndexStorage[indexNames.length];

        for (int i = 0; i < indexStorages.length; i++) {
            String indexName = indexNames[i];

            IndexStorage indexStorage = table.internalTable().storage().getIndex(0, indexId(indexName));

            assertNotNull(indexStorage, indexName);

            indexStorages[i] = indexStorage;
        }

        return indexStorages;
    }

    private static int indexId(String indexName) {
        IgniteImpl node = node();

        return TableTestUtils.getIndexIdStrict(node.catalogManager(), indexName, node.clock().nowLong());
    }

    private static void verifyPutIntoIndexes(IndexStorage... indexStorages) {
        for (IndexStorage indexStorage : indexStorages) {
            verify(indexStorage).put(any());
        }
    }
}
