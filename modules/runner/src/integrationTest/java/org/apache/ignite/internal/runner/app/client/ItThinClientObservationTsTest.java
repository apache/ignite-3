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

package org.apache.ignite.internal.runner.app.client;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

/**
 * This test is checking correctness of observation timestamp calculation.
 */
@SuppressWarnings({"resource", "DataFlowIssue", "RedundantMethodOverride"})
public class ItThinClientObservationTsTest extends ItAbstractThinClientTest {
    @Override
    protected int nodes() {
        return 2;
    }

    @Test
    public void test() {
        Table srvTable = server().tables().table(TABLE_NAME);
        Table clientTable = client().tables().table(TABLE_NAME);

        srvTable.keyValueView().put(null, Tuple.create().set(COLUMN_KEY, 42), Tuple.create().set(COLUMN_VAL, "srv value"));

        clientTable.keyValueView().put(null, Tuple.create().set(COLUMN_KEY, 42), Tuple.create().set(COLUMN_VAL, "client value"));

        Transaction tx =  client().transactions().begin(new TransactionOptions().readOnly(true));

        String clientValue = clientTable.keyValueView().get(tx,  Tuple.create().set(COLUMN_KEY, 42)).value(COLUMN_VAL);

        tx.commit();

        Transaction srvTx =  server().transactions().begin(new TransactionOptions().readOnly(true));

        String srvValue = srvTable.keyValueView().get(srvTx,  Tuple.create().set(COLUMN_KEY, 42)).value(COLUMN_VAL);

        srvTx.commit();

        assertEquals("client value", clientValue, "Values [client=" + clientValue + ", srv=" + srvValue + ']');
        assertEquals("srv value", srvValue, "Values [client=" + clientValue + ", srv=" + srvValue + ']');

        String directClientValue = clientTable.keyValueView().get(null,  Tuple.create().set(COLUMN_KEY, 42)).value(COLUMN_VAL);
        String directSrvValue = srvTable.keyValueView().get(null,  Tuple.create().set(COLUMN_KEY, 42)).value(COLUMN_VAL);

        assertEquals("client value", directClientValue, directClientValue);
        assertEquals("client value", directSrvValue, directSrvValue);
    }

    @Test
    public void testImplicitTxOnDifferentConnections() {
        Table table = client().tables().table(TABLE_NAME);
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        Tuple key = Tuple.create().set(COLUMN_KEY, 69);
        kvView.put(null, key, Tuple.create().set(COLUMN_VAL, "initial"));

        waitNonEmptyPartitionAssignment(table);
        List<String> uniqueNodeIds = getPartitionAssignment(table).stream().distinct().collect(Collectors.toList());
        assertEquals(2, uniqueNodeIds.size(), "Unexpected number of unique node ids");

        for (int i = 0; i < 1000; i++) {
            String valStr = "value " + i;

            kvView.put(null, key, Tuple.create().set(COLUMN_VAL, valStr));

            // Switch partition assignment to a different node.
            setPartitionAssignment(table, uniqueNodeIds.get(i % uniqueNodeIds.size()));

            String val = kvView.get(null, key).value(COLUMN_VAL);

            assertEquals(valStr, val);
        }
    }

    private static void setPartitionAssignment(Table table, String nodeId) {
        List<String> partitions = getPartitionAssignment(table);
        Collections.fill(partitions, nodeId);
    }

    private static void waitNonEmptyPartitionAssignment(Table table) {
        await().until(() -> {
            // Perform table op to trigger assignment change notification.
            table.recordView().contains(null, Tuple.create().set(COLUMN_KEY, 0));
            return !isPartitionAssignmentEmpty(getPartitionAssignment(table));
        });
    }

    private static List<String> getPartitionAssignment(Table table) {
        assertInstanceOf(ClientTable.class, table);

        Object partitionAssignment = IgniteTestUtils.getFieldValue(table, "partitionAssignment");
        CompletableFuture<List<String>> partitionsFut = IgniteTestUtils.getFieldValue(partitionAssignment, "partitionsFut");

        return partitionsFut.join();
    }

    private static boolean isPartitionAssignmentEmpty(List<String> partitionAssignment) {
        for (String nodeId : partitionAssignment) {
            if (nodeId != null) {
                return false;
            }
        }

        return true;
    }
}
