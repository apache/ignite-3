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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Data streamer load test.
 */
public final class ItClientDataStreamerLoadTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test_table";

    private static final int CLIENT_COUNT = 10;

    private static final IgniteClient[] clients = new IgniteClient[CLIENT_COUNT];

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    public static void startClient() {
        for (int i = 0; i < CLIENT_COUNT; i++) {
            clients[i] = IgniteClient.builder().addresses("localhost").build();;
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
    public void testHighLoad() {
        // 4 partitions, 1 parallel ops = ~40s
        // 10 partitions, 1 parallel ops = ~23s
        // 10 partitions, 4 parallel ops = ~25s
        // 10 partitions, 2 parallel ops = ~24s

        RecordView<Tuple> view = defaultTable().recordView();
        view.upsert(null, tuple(2, "_"));
        view.upsert(null, tuple(3, "baz"));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            var options = DataStreamerOptions.builder()
                    .perPartitionParallelOperations(2)
                    .build();

            streamerFut = view.streamData(publisher, options);

            // Insert same data over and over again.
            for (int j = 0; j < 100; j++) {
                for (int i = 0; i < 100_000; i++) {
                    publisher.submit(DataStreamerItem.of(tuple(i, "foo_" + i)));
                }
            }
        }

        streamerFut.orTimeout(10, TimeUnit.SECONDS).join();

        assertNotNull(view.get(null, tupleKey(1)));
        assertNotNull(view.get(null, tupleKey(999)));
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
