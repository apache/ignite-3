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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Common test logic for data streamer - client and server APIs.
 */
public abstract class ItAbstractDataStreamerTest extends ClusterPerClassIntegrationTest {
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

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testBasicStreamingRecordBinaryView(int batchSize) {
        RecordView<Tuple> view = defaultTable().recordView();

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().pageSize(batchSize).build();
            streamerFut = view.streamData(publisher, options);

            publisher.submit(tuple(1, "foo"));
            publisher.submit(tuple(2, "bar"));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNotNull(view.get(null, tupleKey(1)));
        assertNotNull(view.get(null, tupleKey(2)));
        assertNull(view.get(null, tupleKey(3)));

        assertEquals("bar", view.get(null, tupleKey(2)).stringValue("name"));
    }

    @Test
    public void testBasicStreamingRecordPojoView() {
        RecordView<PersonPojo> view = defaultTable().recordView(PersonPojo.class);
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<PersonPojo>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(new PersonPojo(1, "foo"));
            publisher.submit(new PersonPojo(2, "bar"));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, new PersonPojo(2)).name);
    }

    @Test
    public void testBasicStreamingKvBinaryView() {
        KeyValueView<Tuple, Tuple> view = defaultTable().keyValueView();
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Map.Entry<Tuple, Tuple>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(Map.entry(tupleKey(1), Tuple.create().set("name", "foo")));
            publisher.submit(Map.entry(tupleKey(2), Tuple.create().set("name", "bar")));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, tupleKey(2)).stringValue("name"));
    }

    @Test
    public void testBasicStreamingKvPojoView() {
        KeyValueView<Integer, PersonValPojo> view = defaultTable().keyValueView(Mapper.of(Integer.class), Mapper.of(PersonValPojo.class));
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Map.Entry<Integer, PersonValPojo>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(Map.entry(1, new PersonValPojo("foo")));
            publisher.submit(Map.entry(2, new PersonValPojo("bar")));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, 2).name);
    }

    @Test
    public void testAutoFlushByTimer() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().autoFlushFrequency(100).build();
            streamerFut = view.streamData(publisher, options);

            publisher.submit(tuple(1, "foo"));
            waitForKey(view, tupleKey(1));
        }

        assertThat(streamerFut, willSucceedIn(5, TimeUnit.SECONDS));
    }

    @Test
    public void testAutoFlushDisabled() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().autoFlushFrequency(-1).build();
            streamerFut = view.streamData(publisher, options);

            publisher.submit(tuple(1, "foo"));
            assertFalse(waitForCondition(() -> view.get(null, tupleKey(1)) != null, 1000));
        }

        assertThat(streamerFut, willSucceedIn(5, TimeUnit.SECONDS));
    }

    @Test
    public void testMissingKeyColumn() {
        RecordView<Tuple> view = this.defaultTable().recordView();

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().build();
            streamerFut = view.streamData(publisher, options);

            var tuple = Tuple.create();

            publisher.submit(tuple);
        }

        var ex = assertThrows(CompletionException.class, () -> streamerFut.orTimeout(1, TimeUnit.SECONDS).join());
        assertEquals("Missed key column: ID", ex.getCause().getMessage());
    }

    @Test
    public void testManyItems() {
        RecordView<Tuple> view = defaultTable().recordView();

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().pageSize(33).build();
            streamerFut = view.streamData(publisher, options);

            for (int i = 0; i < 10_000; i++) {
                publisher.submit(tuple(i, "x-" + i));
            }
        }

        streamerFut.orTimeout(30, TimeUnit.SECONDS).join();

        assertNotNull(view.get(null, tupleKey(1)));
        assertNotNull(view.get(null, tupleKey(9999)));
        assertNull(view.get(null, tupleKey(10_000)));
    }

    @SuppressWarnings("resource")
    @Test
    public void testSchemaUpdateWhileStreaming() throws InterruptedException {
        Session ses = ignite().sql().createSession();

        String tableName = "testSchemaUpdateWhileStreaming";
        ses.execute(null, "CREATE TABLE " + tableName + "(ID INT NOT NULL PRIMARY KEY)");
        RecordView<Tuple> view = ignite().tables().table(tableName).recordView();

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().pageSize(1).build();
            streamerFut = view.streamData(publisher, options);

            publisher.submit(tupleKey(1));
            waitForKey(view, tupleKey(1));

            ses.execute(null, "ALTER TABLE " + tableName + " ADD COLUMN NAME VARCHAR NOT NULL DEFAULT 'bar'");
            publisher.submit(tupleKey(2));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertEquals("bar", view.get(null, tupleKey(2)).stringValue("name"));
    }

    private void waitForKey(RecordView<Tuple> view, Tuple key) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            @SuppressWarnings("resource")
            var tx = ignite().transactions().begin(new TransactionOptions().readOnly(true));

            try {
                return view.get(tx, key) != null;
            } finally {
                tx.rollback();
            }
        }, 50, 5000));
    }

    private Table defaultTable() {
        //noinspection resource
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

    private static class PersonPojo {
        int id;
        String name;
        Double salary;

        @SuppressWarnings("unused") // Required by serializer.
        private PersonPojo() {
            // No-op.
        }

        PersonPojo(int id) {
            this.id = id;
        }

        PersonPojo(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    private static class PersonValPojo {
        String name;
        Double salary;

        @SuppressWarnings("unused") // Required by serializer.
        private PersonValPojo() {
            // No-op.
        }

        PersonValPojo(String name) {
            this.name = name;
        }
    }
}
