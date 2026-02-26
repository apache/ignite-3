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

package org.apache.ignite.client;

import static org.apache.ignite.client.fakes.FakeIgniteTables.TABLE_ONE_COLUMN;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for client transactions.
 */
public class ClientTransactionsTest extends BaseIgniteAbstractTest {
    private TestServer server;

    @BeforeEach
    public void setUp() {
        server = TestServer.builder().build();
    }

    @AfterEach
    public void tearDown() {
        server.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRollBackDoesNotThrowOnServerDisconnect(boolean async) {
        try (var client = IgniteClient.builder().addresses("127.0.0.1:" + server.port()).build()) {
            ((FakeIgniteTables) server.ignite().tables()).createTable(TABLE_ONE_COLUMN);
            RecordView<String> recView = client.tables().table(TABLE_ONE_COLUMN).recordView(Mapper.of(String.class));
            Transaction tx = client.transactions().begin();
            recView.upsert(tx, "foo");

            server.close();

            if (async) {
                assertThat(tx.rollbackAsync(), willSucceedFast());
            } else {
                assertDoesNotThrow(tx::rollback);
            }
        }
    }
}
