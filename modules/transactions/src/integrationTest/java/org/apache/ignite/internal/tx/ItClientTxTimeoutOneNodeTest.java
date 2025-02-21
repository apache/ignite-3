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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;

import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

class ItClientTxTimeoutOneNodeTest extends ItTxTimeoutOneNodeTest {
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
    InternalTransaction toInternalTransaction(Transaction tx) {
        long txId = ClientLazyTransaction.get(tx).startedTx().id();

        ClientResourceRegistry resources = unwrapIgniteImpl(cluster.aliveNode()).clientInboundMessageHandler().resources();
        try {
            return resources.get(txId).get(InternalTransaction.class);
        } catch (IgniteInternalCheckedException e) {
            throw new RuntimeException(e);
        }
    }
}
