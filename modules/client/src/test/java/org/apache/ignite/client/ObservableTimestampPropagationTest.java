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

import static org.apache.ignite.internal.hlc.HybridTimestamp.LOGICAL_TIME_BITS_SIZE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that observable timestamp (causality token) is propagated from server to client and back.
 */
public class ObservableTimestampPropagationTest extends BaseIgniteAbstractTest {
    private static TestServer testServer;

    private static FakeIgnite ignite;

    private static IgniteClient client;

    private static final AtomicLong currentServerTimestamp = new AtomicLong(1);

    @BeforeAll
    public static void startServer2() {
        TestHybridClock clock = new TestHybridClock(currentServerTimestamp::get);

        ignite = new FakeIgnite("server-2");
        testServer = new TestServer(0, ignite, null, null, "server-2", UUID.randomUUID(), null, null, clock);

        client = IgniteClient.builder().addresses("127.0.0.1:" + testServer.port()).build();
    }

    @AfterAll
    public static void stopServer2() throws Exception {
        IgniteUtils.closeAll(client, testServer, ignite);
    }

    @Test
    @SuppressWarnings("resource")
    public void testClientPropagatesLatestKnownHybridTimestamp() {
        assertNull(lastObservableTimestamp());

        // RW TX does not propagate timestamp.
        client.transactions().begin();
        assertNull(lastObservableTimestamp());

        // RO TX propagates timestamp.
        client.transactions().begin(new TransactionOptions().readOnly(true));
        assertEquals(1, lastObservableTimestamp());

        // Increase timestamp on server - client does not know about it initially.
        currentServerTimestamp.set(11);
        client.transactions().begin(new TransactionOptions().readOnly(true));
        assertEquals(1, lastObservableTimestamp());

        // Subsequent RO TX propagates latest known timestamp.
        client.tables().tables();
        client.transactions().begin(new TransactionOptions().readOnly(true));
        assertEquals(11, lastObservableTimestamp());

        // Smaller timestamp from server is ignored by client.
        currentServerTimestamp.set(9);
        client.transactions().begin(new TransactionOptions().readOnly(true));
        client.transactions().begin(new TransactionOptions().readOnly(true));
        assertEquals(11, lastObservableTimestamp());

        Statement statement = client.sql().statementBuilder()
                .property("hasMorePages", true)
                .query("SELECT 1")
                .build();

        // Execution of a SQL query should propagate observable time, not the current time of the clock.
        currentServerTimestamp.set(20);
        updateObservableTimestamp(14);
        AsyncResultSet<?> rs = await(client.sql().executeAsync(null, statement));
        assertEquals(14, lastObservableTimestamp());

        assertNotNull(rs);

        // Every fetch should propagate observable time, not the current time of the clock.
        currentServerTimestamp.set(20);
        updateObservableTimestamp(18);
        await(rs.fetchNextPage());
        assertEquals(18, lastObservableTimestamp());

        currentServerTimestamp.set(24);
        updateObservableTimestamp(20);
        await(rs.fetchNextPage());
        assertEquals(20, lastObservableTimestamp());

        // Closing a result set should propagate observable time as well.
        updateObservableTimestamp(22);
        await(rs.closeAsync());
        assertEquals(22, lastObservableTimestamp());
    }

    private static @Nullable Long lastObservableTimestamp() {
        HybridTimestamp ts = ignite.timestampTracker().get();

        return ts == null ? null : ts.longValue() >> LOGICAL_TIME_BITS_SIZE;
    }

    private static void updateObservableTimestamp(long newTime) {
        ignite.timestampTracker().update(HybridTimestamp.hybridTimestamp(newTime << LOGICAL_TIME_BITS_SIZE));
    }
}
