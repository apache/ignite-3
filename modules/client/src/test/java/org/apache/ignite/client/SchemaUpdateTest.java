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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.client.ClientMetricSource;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests schema update logic. See also {@link ClientTableTest#testGetReturningTupleWithUnknownSchemaRequestsNewSchema}.
 */
public class SchemaUpdateTest {
    private static final String DEFAULT_TABLE = "default_table";
    private TestServer server;
    private IgniteClient client;

    @AfterEach
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }

        if (server != null) {
            server.close();
        }
    }

    @Test
    public void testMultipleParallelOperationsRequestSchemaOnce() {
        int responseDelay = 100;
        server = new TestServer(10800, 10, 1000, ignite(), null, idx -> responseDelay, "n", UUID.randomUUID(), null);
        client = startClient();

        RecordView<Tuple> view = client.tables().table(DEFAULT_TABLE).recordView();
        CompletableFuture<Void> fut1 = view.upsertAsync(null, Tuple.create().set("id", 1));
        CompletableFuture<Void> fut2 = view.upsertAsync(null, Tuple.create().set("id", 2));

        CompletableFuture.allOf(fut1, fut2).join();
    }

    @NotNull
    @Test
    public void testFailedSchemaLoadTaskIsRetried() {
        // TODO
    }

    private IgniteClient startClient() {
        return IgniteClient.builder()
                .addresses("127.0.0.1:" + server.port())
                .metricsEnabled(true)
                .build();
    }

    private ClientMetricSource metrics() {
        return ((TcpIgniteClient) client).metrics();
    }

    private static FakeIgnite ignite() {
        FakeIgnite ignite = new FakeIgnite();

        ((FakeIgniteTables) ignite.tables()).createTable(DEFAULT_TABLE);

        return ignite;
    }
}
