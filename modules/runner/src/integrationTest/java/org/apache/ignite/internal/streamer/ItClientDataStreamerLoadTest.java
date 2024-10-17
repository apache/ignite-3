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
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Common test logic for data streamer - client and server APIs.
 */
public abstract class ItClientDataStreamerLoadTest extends ClusterPerClassIntegrationTest {
    public static final String TABLE_NAME = "test_table";

    abstract Ignite ignite();

    @BeforeAll
    public void createTable() {
        createTable(TABLE_NAME, 2, 10);
    }

    @BeforeEach
    public void clearTable() {
        sql("DELETE FROM " + TABLE_NAME);
    }

    @Test
    public void testHighLoad() {
        RecordView<Tuple> view = defaultTable().recordView();
        view.upsert(null, tuple(2, "_"));
        view.upsert(null, tuple(3, "baz"));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            var options = DataStreamerOptions.builder()
                    .perPartitionParallelOperations(10)
                    .build();

            streamerFut = view.streamData(publisher, options);

            // Insert same data over and over again.
            for (int j = 0; j < 100_000; j++) {
                for (int i = 0; i < 10_000; i++) {
                    publisher.submit(DataStreamerItem.of(tuple(i, "foo_" + i)));
                }
            }
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNotNull(view.get(null, tupleKey(1)));
        assertNotNull(view.get(null, tupleKey(10_000)));
    }

    private Table defaultTable() {
        return ignite().tables().table(TABLE_NAME);
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
