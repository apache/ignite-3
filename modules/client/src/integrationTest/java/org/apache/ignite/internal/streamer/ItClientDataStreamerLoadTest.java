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

package org.apache.ignite.internal.streamer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Data streamer load test.
 */
public final class ItClientDataStreamerLoadTest extends ClusterPerClassIntegrationTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItClientDataStreamerLoadTest.class);

    private static final String TABLE_NAME = "test_table";

    private static final int CLIENT_COUNT = 2;

    private static final int SERVER_COUNT = 2;

    private static final int ROW_COUNT = 100_000;

    private static final int LOOP_COUNT = 10;

    private static final IgniteClient[] clients = new IgniteClient[CLIENT_COUNT];

    @Override
    protected int initialNodes() {
        return SERVER_COUNT;
    }

    @BeforeAll
    public static void startClient() {
        for (int i = 0; i < CLIENT_COUNT; i++) {
            //noinspection resource
            clients[i] = IgniteClient.builder()
                    .addresses("localhost")
                    .heartbeatInterval(1000)
                    .heartbeatTimeout(2000)
                    .retryPolicy(new RetryLimitPolicy().retryLimit(0))
                    .build();
        }
    }

    @AfterAll
    public static void stopClient() throws Exception {
        IgniteUtils.closeAll(clients);
    }

    @BeforeAll
    public void createTable() {
        createTable(TABLE_NAME, 1, 10);
    }

    @BeforeEach
    public void clearTable() {
        sql("DELETE FROM " + TABLE_NAME);
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    public void testHighLoad() throws InterruptedException {
        IgniteImpl ignite = TestWrappers.unwrapIgniteImpl(node(0));
        boolean invertedWaitOrder = ignite.txManager().lockManager().policy().invertedWaitOrder();

        Thread[] threads = new Thread[CLIENT_COUNT];

        for (int i = 0; i < clients.length; i++) {
            IgniteClient client = clients[i];
            Thread thread = new Thread(() -> streamData(client));
            thread.start();

            threads[i] = thread;
        }

        for (Thread thread : threads) {
            thread.join();
        }

        RecordView<Tuple> view = clients[0].tables().table(TABLE_NAME).recordView();

        List<Tuple> keys = new ArrayList<>(ROW_COUNT);

        for (int i = 0; i < ROW_COUNT; i++) {
            Tuple key = tupleKey(i);
            keys.add(key);
        }

        List<Tuple> values = view.getAll(null, keys);
        assertEquals(ROW_COUNT, values.size());

        for (int i = 0; i < ROW_COUNT; i++) {
            Tuple res = values.get(i);

            // TODO https://issues.apache.org/jira/browse/IGNITE-28365
            // A row might be missing in the following scenario (assuming 2 concurrent streamers):
            // batch 1 is concurrently mapped to partition K, streamer 0 wins the conflict
            // batch 2 is concurrently mapped to partition N, streamer 1 wins the conflict
            // Both streamers become invalidated without proper implicit retries and stop.
            if (res == null && !invertedWaitOrder) {
                continue;
            }

            assertNotNull(res, "Row not found: " + i);
            assertEquals("foo_" + i, res.value("name"));
        }
    }

    private static void streamData(IgniteClient client) {
        RecordView<Tuple> view = client.tables().table(TABLE_NAME).recordView();
        CompletableFuture<Void> streamerFut;
        Random rnd = new Random();

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            var options = DataStreamerOptions.builder()
                    .perPartitionParallelOperations(rnd.nextInt(2) + 1)
                    .pageSize(rnd.nextInt(1000) + 100)
                    .retryLimit(1)
                    .build();

            streamerFut = view.streamData(publisher, options);

            // Insert same data over and over again.
            for (int j = 0; j < LOOP_COUNT; j++) {
                LOG.info("Loop " + j);
                for (int i = 0; i < ROW_COUNT; i++) {
                    publisher.submit(DataStreamerItem.of(tuple(i, "foo_" + i)));
                }
            }
        }

        try {
            streamerFut.orTimeout(10, TimeUnit.SECONDS).join();
            LOG.info("Done streaming");
        } catch (Exception e) {
            // TODO IGNITE-28365 Don't expecting errors here with proper retries
            LOG.warn("Done streaming with error", e);
        }
    }

    private static Tuple tuple(int id, String name) {
        return Tuple.create()
                .set("id", id)
                .set("name", name);
    }

    private static Tuple tupleKey(int id) {
        return Tuple.create()
                .set("id", id);
    }
}
