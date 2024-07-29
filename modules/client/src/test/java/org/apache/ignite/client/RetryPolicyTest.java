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

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.client.ClientUtils;
import org.apache.ignite.internal.client.IgniteClientConfigurationImpl;
import org.apache.ignite.internal.client.RetryPolicyContextImpl;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.LoggerFactory;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests thin client retry behavior.
 */
public class RetryPolicyTest extends BaseIgniteAbstractTest {
    private static final int ITER = 100;

    private TestServer server;

    @AfterEach
    void tearDown() throws Exception {
        closeAll(server);
    }

    @Test
    public void testNoRetryPolicySecondRequestFails() throws Exception {
        initServer(reqId -> reqId % 3 == 0);

        try (var client = getClient(null)) {
            assertEquals("t", client.tables().tables().get(0).name());
            assertThrows(IgniteException.class, () -> client.tables().tables().get(0).name());
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
            assertThrows(IgniteException.class, () -> client.tables().table(FakeIgniteTables.BAD_TABLE));
            assertEquals(0, plc.invocations.size());
        }
    }

    @Test
    public void testRetryPolicyDoesNotRetryTxCommit() throws Exception {
        initServer(reqId -> reqId % 3 == 0);
        var plc = new TestRetryPolicy();

        try (var client = getClient(plc)) {
            Transaction tx = client.transactions().begin();
            ClientLazyTransaction.ensureStarted(tx, ((TcpIgniteClient) client).channel(), null).join();

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
            assertThrows(IgniteException.class, () -> client.tables().tables());
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
            assertThrows(IgniteException.class, () -> client.tables().tables());
        }

        assertEquals(3, plc.invocations.size());

        RetryPolicyContext ctx = plc.invocations.get(1);

        assertEquals(1, ctx.iteration());
        assertEquals(ClientOperationType.TABLES_GET, ctx.operation());
        assertSame(plc, ctx.configuration().retryPolicy());
        assertThat(ctx.exception().getMessage(), containsString("Channel is closed"));
    }

    @Test
    public void testTableOperationWithoutTxIsRetried() throws Exception {
        initServer(reqId -> reqId % 4 == 0);
        var plc = new TestRetryPolicy();

        try (var client = getClient(plc)) {
            RecordView<Tuple> recView = client.tables().table("t").recordView();
            recView.get(null, Tuple.create().set("id", 1L));
            recView.get(null, Tuple.create().set("id", 1L));

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
            ClientLazyTransaction.ensureStarted(tx, ((TcpIgniteClient) client).channel(), null).join();

            var ex = assertThrows(IgniteException.class, () -> recView.get(tx, Tuple.create().set("id", 1)));
            assertThat(ex.getMessage(), containsString("Transaction context has been lost due to connection errors."));

            assertEquals(0, plc.invocations.size());
        }
    }

    @Test
    public void testRetryReadPolicyRetriesReadOperations() throws Exception {
        // Standard requests are:
        // 1: Handshake
        // 2: SCHEMAS_GET
        // 3: PARTITION_ASSIGNMENT_GET
        // => fail on 4th request
        initServer(reqId -> reqId % 4 == 0);

        var loggerFactory = new TestLoggerFactory("c");

        try (var client = getClient(new RetryReadPolicy(), loggerFactory)) {
            RecordView<Tuple> recView = client.tables().table("t").recordView();
            recView.get(null, Tuple.create().set("id", 1L));
            recView.get(null, Tuple.create().set("id", 1L));

            loggerFactory.assertLogContains("Connection closed");
            loggerFactory.assertLogContains("Retrying operation [opCode=12, opType=TUPLE_GET, attempt=0, lastError=java.util");
        }
    }

    @Test
    public void testRetryReadPolicyDoesNotRetryWriteOperations() throws Exception {
        initServer(reqId -> reqId % 6 == 0);

        try (var client = getClient(new RetryReadPolicy())) {
            RecordView<Tuple> recView = client.tables().table("t").recordView();
            recView.upsert(null, Tuple.create().set("id", 1L));
            assertThrows(IgniteClientConnectionException.class, () -> recView.upsert(null, Tuple.create().set("id", 1L)));
        }
    }

    @Test
    public void testRetryPolicyConvertOpAllOperationsSupported() throws IllegalAccessException {
        var nullOpFields = new ArrayList<String>();

        for (var field : ClientOp.class.getDeclaredFields()) {
            var opCode = (int) field.get(null);

            if (opCode == ClientOp.RESERVED_EXTENSION_RANGE_START || opCode == ClientOp.RESERVED_EXTENSION_RANGE_END) {
                continue;
            }

            var publicOp = ClientUtils.opCodeToClientOperationType(opCode);

            if (publicOp == null) {
                nullOpFields.add(field.getName());
            }
        }

        long expectedNullCount = 21;

        String msg = nullOpFields.size()
                + " operation codes do not have public equivalent. When adding new codes, update ClientOperationType too. Missing ops: "
                + String.join(", ", nullOpFields);

        assertEquals(expectedNullCount, nullOpFields.size(), msg);
    }

    @Test
    public void testRetryReadPolicyAllOperationsSupported() {
        var plc = new RetryReadPolicy();
        var cfg = new IgniteClientConfigurationImpl(null, null, 0, 0, 0, 0, null, 0, 0, null, null, null, false, null, 0);

        for (var op : ClientOperationType.values()) {
            var ctx = new RetryPolicyContextImpl(cfg, op, 0, null);
            plc.shouldRetry(ctx);
        }
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

    @Test
    public void testExceptionInRetryPolicyPropagatesToCaller() throws Exception {
        initServer(reqId -> reqId % 2 == 0);
        var plc = new TestRetryPolicy();
        plc.shouldThrow = true;

        try (var client = getClient(plc)) {
            IgniteException ex = assertThrows(IgniteException.class, () -> client.tables().tables());
            var cause = (RuntimeException) ex.getCause();

            assertEquals("TestRetryPolicy exception.", cause.getMessage());
        }
    }

    private IgniteClient getClient(@Nullable RetryPolicy retryPolicy) {
        return getClient(retryPolicy, null);
    }

    private IgniteClient getClient(@Nullable RetryPolicy retryPolicy, @Nullable LoggerFactory loggerFactory) {
        return IgniteClient.builder()
                .addresses("127.0.0.1:" + server.port())
                .retryPolicy(retryPolicy)
                .reconnectThrottlingPeriod(0)
                .loggerFactory(loggerFactory)
                .build();
    }

    private void initServer(Function<Integer, Boolean> shouldDropConnection) {
        FakeIgnite ign = new FakeIgnite();
        ((FakeIgniteTables) ign.tables()).createTable("t");

        server = new TestServer(0, ign, shouldDropConnection, null, null, UUID.randomUUID(), null, null);
    }
}
