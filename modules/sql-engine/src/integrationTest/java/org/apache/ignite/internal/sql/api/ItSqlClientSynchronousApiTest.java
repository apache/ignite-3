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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.List;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.handler.ClientInboundMessageHandler;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.IgniteTransactions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for synchronous client SQL API.
 */
public class ItSqlClientSynchronousApiTest extends ItSqlSynchronousApiTest {
    private IgniteClient client;

    @BeforeAll
    public void startClient() {
        client = buildClient();
    }

    @AfterAll
    public void stopClient() {
        client.close();
    }

    @AfterEach
    public void checkResourceCleanup() {
        ClientInboundMessageHandler handler = unwrapIgniteImpl(CLUSTER.aliveNode()).clientInboundMessageHandler();

        Awaitility.await().untilAsserted(() -> assertThat(handler.cancelHandlesCount(), is(0)));
    }

    @Override
    protected IgniteSql igniteSql() {
        return client.sql();
    }

    @Override
    protected IgniteTransactions igniteTx() {
        return client.transactions();
    }

    @Test
    void cursorCloseIgnoresErrorOnClientDisconnect() {
        try (IgniteClient client0 = buildClient()) {
            Statement stmt = client0.sql().statementBuilder()
                    .query("SELECT * FROM TABLE(SYSTEM_RANGE(0, 1))")
                    .pageSize(1)
                    .build();

            ResultSet<SqlRow> rs = client0.sql().execute(null, stmt);

            client0.close();

            assertDoesNotThrow(rs::close);
        }
    }

    private static IgniteClient buildClient() {
        return IgniteClient
                .builder()
                .addresses(getClientAddresses(List.of(CLUSTER.aliveNode())).get(0))
                .build();
    }
}
