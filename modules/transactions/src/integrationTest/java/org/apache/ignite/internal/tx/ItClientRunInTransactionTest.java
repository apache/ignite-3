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
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.client.sql.PartitionMappingProvider;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

class ItClientRunInTransactionTest extends ItRunInTransactionTest {
    private IgniteClient client;

    @BeforeEach
    void startClient() {
        client = IgniteClient.builder()
                .addresses("localhost:" + unwrapIgniteImpl(cluster.aliveNode()).clientAddress().port())
                .build();
    }

    @AfterEach
    void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    Ignite ignite() {
        return client;
    }

    @Override
    void initAwareness(Table table) {
        Tuple key0 = key(0);
        ClientSql sql = (ClientSql) ignite().sql();
        sql.execute(format("INSERT INTO %s (%s, %s) VALUES (?, ?)", TABLE_NAME, COLUMN_KEY, COLUMN_VAL),
                key0.intValue(0), key0.intValue(0) + "");

        sql.partitionAwarenessCachedMetas().stream().allMatch(PartitionMappingProvider::ready);
    }

    @Override
    UUID txId(Transaction tx) {
        ClientLazyTransaction tx0 = ClientLazyTransaction.get(tx);
        return tx0.startedTx().txId();
    }
}
