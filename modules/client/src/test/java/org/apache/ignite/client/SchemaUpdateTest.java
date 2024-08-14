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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.client.ClientMetricSource;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests schema update logic. See also {@link ClientTableTest#testGetReturningTupleWithUnknownSchemaRequestsNewSchema}.
 */
public class SchemaUpdateTest extends BaseIgniteAbstractTest {
    private static final String DEFAULT_TABLE = "default_table";
    private TestServer server;
    private IgniteClient client;

    @AfterEach
    public void tearDown() {
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
        server = new TestServer(10000, ignite(), null, idx -> responseDelay, "n", UUID.randomUUID(), null, null);
        client = startClient();

        RecordView<Tuple> view = client.tables().table(DEFAULT_TABLE).recordView();
        long requestsBefore = metrics().requestsSent();

        CompletableFuture<Void> fut1 = view.upsertAsync(null, Tuple.create().set("id", 1L));
        CompletableFuture<Void> fut2 = view.upsertAsync(null, Tuple.create().set("id", 2L));

        CompletableFuture.allOf(fut1, fut2).join();

        // PARTITION_ASSIGNMENT_GET, SCHEMAS_GET, TUPLE_UPSERT x2
        assertEquals(4, metrics().requestsSent() - requestsBefore);
    }

    @Test
    public void testFailedSchemaLoadFutureIsRetried() {
        AtomicBoolean shouldFail = new AtomicBoolean(true);
        Function<Integer, Boolean> shouldDropConnection = idx -> idx >= 3 && shouldFail.getAndSet(false);

        server = new TestServer(10000, ignite(), shouldDropConnection, null, "n", UUID.randomUUID(), null, null);
        client = startClient();

        RecordView<Tuple> view = client.tables().table(DEFAULT_TABLE).recordView();

        try {
            view.upsertAsync(null, Tuple.create().set("id", 1L)).join();
        } catch (Exception ignored) {
            // Expected.
        }

        view.upsertAsync(null, Tuple.create().set("id", 2L)).join();

        TestLoggerFactory testLoggerFactory = (TestLoggerFactory) client.configuration().loggerFactory();

        //noinspection DataFlowIssue
        testLoggerFactory.assertLogContains("Retrying operation [opCode=" + ClientOp.SCHEMAS_GET);
    }

    private IgniteClient startClient() {
        return IgniteClient.builder()
                .addresses("127.0.0.1:" + server.port())
                .metricsEnabled(true)
                .loggerFactory(new TestLoggerFactory("client"))
                .retryPolicy(new RetryLimitPolicy().retryLimit(0))
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
