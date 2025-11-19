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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

/**
 * This test is checking correctness of observation timestamp calculation.
 */
@SuppressWarnings({"resource", "DataFlowIssue"})
public class ItThinClientObservationTsTest extends ItAbstractThinClientTest {
    @Override
    protected long idleSafeTimePropagationDuration() {
        return 10_000L;
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
        Tuple key = Tuple.create().set(COLUMN_KEY, 69);

        for (int i = 0; i < 10; i++) {
            String valStr = "value " + i;

            table.keyValueView().put(null, key, Tuple.create().set(COLUMN_VAL, valStr));
            shiftPartitionAssignment(table);
            String val = table.keyValueView().get(null, key).value(COLUMN_VAL);

            assertEquals(valStr, val);
        }
    }

    private static void shiftPartitionAssignment(Table table) {
        assertInstanceOf(ClientTable.class, table);

        Object partitionAssignment = IgniteTestUtils.getFieldValue(table, "partitionAssignment");
        CompletableFuture<List<String>> partitionsFut = IgniteTestUtils.getFieldValue(partitionAssignment, "partitionsFut");
        List<String> partitions = partitionsFut.join();

        // Shift every partition position to the left by one.
        String first = partitions.remove(0);
        partitions.add(first);
    }
}
