/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.function.Function;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.client.ClientUtils;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests thin client retry behavior.
 */
public class RetryPolicyTest {
    private static final int ITER = 100;

    private TestServer server;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server);
    }

    @Test
    public void testNoRetryPolicySecondRequestFails() throws Exception {
        initServer(reqId -> reqId % 3 == 0);

        try (var client = getClient(null)) {
            assertEquals("t", client.tables().tables().get(0).name());
            assertThrows(IgniteClientException.class, () -> client.tables().tables().get(0).name());
        }
    }

    @Test
    public void testRetryPolicyCompletesOperationWithoutException() throws Exception {
        // Every 3 network message fails, including handshake.
        initServer(reqId -> reqId % 4 == 0);

        var plc = new TestRetryPolicy();
        plc.retryLimit(1);

        try (var client = getClient(plc)) {
            for (int i = 0; i < ITER; i++) {
                assertEquals("t", client.tables().tables().get(0).name());
            }

            assertEquals(ITER / 2 - 1, plc.invocations.size());
        }
    }

    @Test
    public void testRetryPolicyDoesNotRetryUnrelatedErrors() throws Exception {
        initServer(reqId -> reqId % 33 == 0);
        var plc = new TestRetryPolicy();

        try (var client = getClient(plc)) {
            assertThrows(IgniteClientException.class, () -> client.tables().table(FakeIgniteTables.BAD_TABLE));
            assertEquals(0, plc.invocations.size());
        }
    }

    @Test
    public void testRetryPolicyDoesNotRetryTxCommit() throws Exception {
        initServer(reqId -> reqId % 3 == 0);
        var plc = new TestRetryPolicy();

        try (var client = getClient(plc)) {
            Transaction tx = client.transactions().begin();

            assertThrows(IgniteClientConnectionException.class, tx::commit);
            assertEquals(0, plc.invocations.size());
        }
    }

    @Test
    public void testRetryLimitPolicyThrowsOnLimitExceeded() throws Exception {
        initServer(reqId -> reqId % 2 == 0);
        var plc = new TestRetryPolicy();
        plc.retryLimit(5);

        try (var client = getClient(plc)) {
            assertThrows(IgniteClientException.class, () -> client.tables().tables());
        }

        assertEquals(6, plc.invocations.size());
        assertEquals(5, plc.invocations.get(5).iteration());
    }

    @Test
    public void testCustomRetryPolicyIsInvokedWithCorrectContext() throws Exception {
        initServer(reqId -> reqId % 2 == 0);
        var plc = new TestRetryPolicy();
        plc.retryLimit(2);

        try (var client = getClient(plc)) {
            assertThrows(IgniteClientException.class, () -> client.tables().tables());
        }

        assertEquals(3, plc.invocations.size());

        RetryPolicyContext ctx = plc.invocations.get(1);

        assertEquals(1, ctx.iteration());
        assertEquals(ClientOperationType.TABLES_GET, ctx.operation());
        assertSame(plc, ctx.configuration().retryPolicy());
        assertEquals("Channel is closed", ctx.exception().getMessage());
    }

    @Test
    public void testTableOperationWithoutTxIsRetried() throws Exception {
        initServer(reqId -> reqId % 4 == 0);
        var plc = new TestRetryPolicy();

        try (var client = getClient(plc)) {
            RecordView<Tuple> recView = client.tables().table("t").recordView();
            recView.get(null, Tuple.create().set("id", 1));
            recView.get(null, Tuple.create().set("id", 1));

            assertEquals(1, plc.invocations.size());
        }
    }

    @Test
    public void testTableOperationWithTxIsNotRetried() throws Exception {
        initServer(reqId -> reqId % 4 == 0);
        var plc = new TestRetryPolicy();

        try (var client = getClient(plc)) {
            RecordView<Tuple> recView = client.tables().table("t").recordView();
            Transaction tx = client.transactions().begin();

            var ex = assertThrows(IgniteClientException.class, () -> recView.get(tx, Tuple.create().set("id", 1)));
            assertEquals("Transaction context has been lost due to connection errors.", ex.getMessage());

            assertEquals(0, plc.invocations.size());
        }
    }

    @Test
    public void testRetryReadPolicyRetriesReadOperations() throws Exception {
        initServer(reqId -> reqId % 3 == 0);

        try (var client = getClient(new RetryReadPolicy())) {
            RecordView<Tuple> recView = client.tables().table("t").recordView();
            recView.get(null, Tuple.create().set("id", 1));
            recView.get(null, Tuple.create().set("id", 1));
        }
    }

    @Test
    public void testRetryReadPolicyDoesNotRetryWriteOperations() throws Exception {
        initServer(reqId -> reqId % 5 == 0);

        try (var client = getClient(new RetryReadPolicy())) {
            RecordView<Tuple> recView = client.tables().table("t").recordView();
            recView.upsert(null, Tuple.create().set("id", 1));
            assertThrows(IgniteClientConnectionException.class, () -> recView.upsert(null, Tuple.create().set("id", 1)));
        }
    }

    @Test
    public void testRetryPolicyConvertOpAllOperationsSupported() throws IllegalAccessException {
        var nullOpFields = new ArrayList<String>();

        for (var field : ClientOp.class.getDeclaredFields()) {
            var opCode = (int) field.get(null);
            var publicOp = ClientUtils.opCodeToClientOperationType(opCode);

            if (publicOp == null) {
                nullOpFields.add(field.getName());
            }
        }

        long expectedNullCount = 17;

        String msg = nullOpFields.size()
                + " operation codes do not have public equivalent. When adding new codes, update ClientOperationType too. Missing ops: "
                + String.join(", ", nullOpFields);

        assertEquals(expectedNullCount, nullOpFields.size(), msg);
    }

    @Test
    public void testDefaultRetryPolicyIsRetryReadPolicyWithLimit() throws Exception {
        initServer(reqId -> false);

        try (var client = IgniteClient.builder().addresses("127.0.0.1:" + server.port()).build()) {
            var plc = client.configuration().retryPolicy();

            var readPlc = assertInstanceOf(RetryReadPolicy.class, plc);
            assertEquals(RetryReadPolicy.DFLT_RETRY_LIMIT, readPlc.retryLimit());
        }
    }

    private IgniteClient getClient(RetryPolicy retryPolicy) {
        return IgniteClient.builder()
                .addresses("127.0.0.1:" + server.port())
                .retryPolicy(retryPolicy)
                .reconnectThrottlingPeriod(0)
                .build();
    }

    private void initServer(Function<Integer, Boolean> shouldDropConnection) {
        FakeIgnite ign = new FakeIgnite();
        ign.tables().createTable("t", c -> {});

        server = new TestServer(10900, 10, 0, ign, shouldDropConnection, null);
    }
}
