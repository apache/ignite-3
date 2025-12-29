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

import java.time.Duration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

/**
 * This test is checking correctness of observation timestamp calculation.
 */
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
    public void testObservableTsUpdatesOnHeartbeat() {
        Builder clientBuilder = IgniteClient.builder()
                .addresses(getClientAddresses().toArray(new String[0]))
                .heartbeatInterval(100);

        try (IgniteClient client = clientBuilder.build()) {
            HybridTimestampTracker tsTracker = ((TcpIgniteClient) client).channel().observableTimestamp();

            long initialTs = tsTracker.getLong();

            await().atMost(Duration.ofSeconds(5)).until(() -> tsTracker.getLong() > initialTs);
        }
    }
}
