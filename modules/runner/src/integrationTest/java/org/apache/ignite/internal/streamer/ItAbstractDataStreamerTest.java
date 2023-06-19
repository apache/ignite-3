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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
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

        var publisher = new SubmissionPublisher<Tuple>();
        var options = DataStreamerOptions.builder().batchSize(batchSize).build();
        CompletableFuture<Void> streamerFut = view.streamData(publisher, options);

        publisher.submit(tuple(1, "foo"));
        publisher.submit(tuple(2, "bar"));

        publisher.close();
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

            publisher.submit(Map.entry(tupleKey(1), tuple(1, "foo")));
            publisher.submit(Map.entry(tupleKey(2), tuple(2, "bar")));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, tupleKey(2)).stringValue("name"));
    }

    @Test
    public void testBasicStreamingKvPojoView() {
        KeyValueView<Integer, PersonPojo> view = defaultTable().keyValueView(Mapper.of(Integer.class), Mapper.of(PersonPojo.class));
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Map.Entry<Integer, PersonPojo>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(Map.entry(1, new PersonPojo(1, "foo")));
            publisher.submit(Map.entry(2, new PersonPojo(2, "bar")));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, 2).name);
    }

    @Test
    public void testAutoFlushByTimer() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();

        var publisher = new SubmissionPublisher<Tuple>();
        var options = DataStreamerOptions.builder().autoFlushFrequency(100).build();
        view.streamData(publisher, options);

        publisher.submit(tuple(1, "foo"));
        assertTrue(waitForCondition(() -> view.get(null, tupleKey(1)) != null, 1000));
    }

    @Test
    public void testAutoFlushDisabled() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();

        var publisher = new SubmissionPublisher<Tuple>();
        var options = DataStreamerOptions.builder().autoFlushFrequency(-1).build();
        view.streamData(publisher, options);

        publisher.submit(tuple(1, "foo"));
        assertFalse(waitForCondition(() -> view.get(null, tupleKey(1)) != null, 1000));
    }

    @Test
    public void testMissingKeyColumn() {
        RecordView<Tuple> view = this.defaultTable().recordView();

        var publisher = new SubmissionPublisher<Tuple>();
        var options = DataStreamerOptions.builder().build();
        CompletableFuture<Void> streamerFut = view.streamData(publisher, options);

        var tuple = Tuple.create()
                .set("id1", 1)
                .set("name1", "x");

        publisher.submit(tuple);
        publisher.close();
        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
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

    protected static class PersonPojo {
        public int id;
        public String name;

        public PersonPojo() {
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
}
