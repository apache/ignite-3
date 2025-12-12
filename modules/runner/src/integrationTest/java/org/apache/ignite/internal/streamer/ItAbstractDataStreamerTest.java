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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.marshalling.ByteArrayMarshaller;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerException;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOperationType;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.DataStreamerTarget;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.ReceiverDescriptor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Common test logic for data streamer - client and server APIs.
 */
@SuppressWarnings("DataFlowIssue")
public abstract class ItAbstractDataStreamerTest extends ClusterPerClassIntegrationTest {
    public static final String TABLE_NAME = "test_table";

    private static final String TABLE_NAME_COMPOSITE_KEY = "test_table_composite_key";

    abstract Ignite ignite();

    @BeforeAll
    public void createTable() {
        createTable(TABLE_NAME, 2, 10);
        sql("CREATE TABLE test_table_composite_key (\n"
                + "    name VARCHAR,\n"
                + "    data VARCHAR,\n"
                + "    uniqueId VARCHAR,\n"
                + "    foo VARCHAR,\n"
                + "    PRIMARY KEY (uniqueId, name)\n"
                + ")");
    }

    @BeforeEach
    public void clearTable() {
        sql("DELETE FROM " + TABLE_NAME);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testBasicStreamingRecordBinaryView(int batchSize) {
        RecordView<Tuple> view = defaultTable().recordView();
        view.upsert(null, tuple(2, "_"));
        view.upsert(null, tuple(3, "baz"));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            var options = DataStreamerOptions.builder().pageSize(batchSize).build();
            streamerFut = view.streamData(publisher, options);

            publisher.submit(DataStreamerItem.of(tuple(1, "foo")));
            publisher.submit(DataStreamerItem.of(tuple(2, "bar")));
            publisher.submit(DataStreamerItem.removed(tupleKey(3)));
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
        view.upsert(null, new PersonPojo(2, "_"));
        view.upsert(null, new PersonPojo(3, "baz"));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<PersonPojo>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(DataStreamerItem.of(new PersonPojo(1, "foo")));
            publisher.submit(DataStreamerItem.of(new PersonPojo(2, "bar")));
            publisher.submit(DataStreamerItem.removed(new PersonPojo(3)));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertEquals("foo", view.get(null, new PersonPojo(1)).name);
        assertEquals("bar", view.get(null, new PersonPojo(2)).name);
        assertNull(view.get(null, new PersonPojo(3)));
    }

    @Test
    public void testBasicStreamingKvBinaryView() {
        KeyValueView<Tuple, Tuple> view = defaultTable().keyValueView();
        view.put(null, tupleKey(2), Tuple.create().set("name", "_"));
        view.put(null, tupleKey(3), Tuple.create().set("name", "baz"));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Map.Entry<Tuple, Tuple>>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(DataStreamerItem.of(Map.entry(tupleKey(1), Tuple.create().set("name", "foo"))));
            publisher.submit(DataStreamerItem.of(Map.entry(tupleKey(2), Tuple.create().set("name", "bar"))));
            publisher.submit(DataStreamerItem.removed(Map.entry(tupleKey(3), Tuple.create())));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertEquals("foo", view.get(null, tupleKey(1)).stringValue("name"));
        assertEquals("bar", view.get(null, tupleKey(2)).stringValue("name"));
        assertNull(view.get(null, tupleKey(3)));
    }

    @Test
    public void testBasicStreamingKvPojoView() {
        KeyValueView<Integer, PersonValPojo> view = defaultTable().keyValueView(Mapper.of(Integer.class), Mapper.of(PersonValPojo.class));
        view.put(null, 2, new PersonValPojo("_"));
        view.put(null, 3, new PersonValPojo("baz"));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Map.Entry<Integer, PersonValPojo>>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(DataStreamerItem.of(Map.entry(1, new PersonValPojo("foo"))));
            publisher.submit(DataStreamerItem.of(Map.entry(2, new PersonValPojo("bar"))));
            publisher.submit(DataStreamerItem.removed(Map.entry(3, new PersonValPojo("_"))));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertEquals("foo", view.get(null, 1).name);
        assertEquals("bar", view.get(null, 2).name);
        assertNull(view.get(null, 3));
    }

    @Test
    public void testBasicStreamingCompositeKeyRecordBinaryView() {
        RecordView<Tuple> view = compositeKeyTable().recordView();
        view.upsert(null, compositeKeyTuple(1));
        view.upsert(null, compositeKeyTuple(2));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(DataStreamerItem.of(compositeKeyTuple(3)));
            publisher.submit(DataStreamerItem.of(compositeKeyTuple(4)));

            publisher.submit(DataStreamerItem.removed(compositeKeyTupleKey(1)));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNull(view.get(null, compositeKeyTupleKey(1)));
        assertNotNull(view.get(null, compositeKeyTupleKey(2)));
        assertNotNull(view.get(null, compositeKeyTupleKey(3)));
        assertNotNull(view.get(null, compositeKeyTupleKey(4)));

        Tuple resTuple = view.get(null, compositeKeyTupleKey(3));
        assertEquals("name3", resTuple.stringValue("name"));
        assertEquals("data3", resTuple.stringValue("data"));
        assertEquals("uniqueId3", resTuple.stringValue("uniqueId"));
        assertEquals("foo3", resTuple.stringValue("foo"));
    }

    @Test
    public void testBasicStreamingCompositeKeyRecordPojoView() {
        RecordView<CompositeKeyPojo> view = compositeKeyTable().recordView(CompositeKeyPojo.class);
        view.upsert(null, new CompositeKeyPojo(1, "data1", "foo1"));
        view.upsert(null, new CompositeKeyPojo(2, "data2", "foo2"));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<CompositeKeyPojo>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(DataStreamerItem.of(new CompositeKeyPojo(3, "data3", "foo3")));
            publisher.submit(DataStreamerItem.of(new CompositeKeyPojo(4, "data4", "foo4")));

            publisher.submit(DataStreamerItem.removed(new CompositeKeyPojo(1)));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNull(view.get(null, new CompositeKeyPojo(1)));
        assertNotNull(view.get(null, new CompositeKeyPojo(2)));
        assertNotNull(view.get(null, new CompositeKeyPojo(3)));
        assertNotNull(view.get(null, new CompositeKeyPojo(4)));

        CompositeKeyPojo resPojo = view.get(null, new CompositeKeyPojo(3));
        assertEquals("name3", resPojo.name);
        assertEquals("data3", resPojo.data);
        assertEquals("uniqueId3", resPojo.uniqueId);
        assertEquals("foo3", resPojo.foo);
    }

    @Test
    public void testBasicStreamingCompositeKeyKvBinaryView() {
        KeyValueView<Tuple, Tuple> view = compositeKeyTable().keyValueView();
        view.put(null, compositeKeyTupleKey(1), compositeKeyTupleVal(1));
        view.put(null, compositeKeyTupleKey(2), compositeKeyTupleVal(2));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Map.Entry<Tuple, Tuple>>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(DataStreamerItem.of(Map.entry(compositeKeyTupleKey(3), compositeKeyTupleVal(3))));
            publisher.submit(DataStreamerItem.of(Map.entry(compositeKeyTupleKey(4), compositeKeyTupleVal(4))));

            publisher.submit(DataStreamerItem.removed(Map.entry(compositeKeyTupleKey(1), Tuple.create())));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNull(view.get(null, compositeKeyTupleKey(1)));
        assertNotNull(view.get(null, compositeKeyTupleKey(2)));
        assertNotNull(view.get(null, compositeKeyTupleKey(3)));
        assertNotNull(view.get(null, compositeKeyTupleKey(4)));

        Tuple resValue = view.get(null, compositeKeyTupleKey(3));
        assertEquals("data3", resValue.stringValue("data"));
        assertEquals("foo3", resValue.stringValue("foo"));
    }

    @Test
    public void testBasicStreamingCompositeKeyKvPojoView() {
        KeyValueView<CompositeKeyKeyPojo, CompositeKeyValPojo> view = compositeKeyTable().keyValueView(
                Mapper.of(CompositeKeyKeyPojo.class), Mapper.of(CompositeKeyValPojo.class));
        view.put(null, new CompositeKeyKeyPojo(1), new CompositeKeyValPojo(1));
        view.put(null, new CompositeKeyKeyPojo(2), new CompositeKeyValPojo(2));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Map.Entry<CompositeKeyKeyPojo, CompositeKeyValPojo>>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(DataStreamerItem.of(Map.entry(new CompositeKeyKeyPojo(3), new CompositeKeyValPojo(3))));
            publisher.submit(DataStreamerItem.of(Map.entry(new CompositeKeyKeyPojo(4), new CompositeKeyValPojo(4))));

            publisher.submit(DataStreamerItem.removed(Map.entry(new CompositeKeyKeyPojo(1), new CompositeKeyValPojo(1))));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNull(view.get(null, new CompositeKeyKeyPojo(1)));
        assertNotNull(view.get(null, new CompositeKeyKeyPojo(2)));
        assertNotNull(view.get(null, new CompositeKeyKeyPojo(3)));
        assertNotNull(view.get(null, new CompositeKeyKeyPojo(4)));

        CompositeKeyValPojo resValue = view.get(null, new CompositeKeyKeyPojo(3));
        assertEquals("data3", resValue.data);
        assertEquals("foo3", resValue.foo);
    }

    @Test
    public void testAutoFlushByTimer() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SimplePublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().autoFlushInterval(100).build();
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

        try (var publisher = new SimplePublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().autoFlushInterval(-1).build();
            streamerFut = view.streamData(publisher, options);

            publisher.submit(tuple(1, "foo"));
            assertFalse(waitForCondition(() -> view.get(null, tupleKey(1)) != null, 1000));
        }

        assertThat(streamerFut, willSucceedIn(5, TimeUnit.SECONDS));
    }

    @Test
    public void testMissingKeyColumn() {
        RecordView<Tuple> view = this.defaultTable().recordView();

        DataStreamerItem<Tuple> item = DataStreamerItem.of(Tuple.create());
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            var options = DataStreamerOptions.builder().build();
            streamerFut = view.streamData(publisher, options);

            publisher.submit(item);
        }

        var ex = assertThrows(CompletionException.class, () -> streamerFut.orTimeout(1, TimeUnit.SECONDS).join());
        assertThat(ex.getMessage(), containsString("Missed key column: ID"));

        DataStreamerException cause = (DataStreamerException) ex.getCause();
        assertEquals(1, cause.failedItems().size());
        assertEquals(item, cause.failedItems().iterator().next());
    }

    @SuppressWarnings("Convert2MethodRef")
    @Test
    public void testManyItems() {
        int count = 5_000;

        RecordView<Tuple> view = defaultTable().recordView();
        view.upsertAll(null, IntStream.range(0, count).mapToObj(i -> tuple(i, "old-" + i)).collect(Collectors.toList()));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            var options = DataStreamerOptions.builder().pageSize(33).build();
            streamerFut = view.streamData(publisher, options);

            for (int i = 0; i < count; i++) {
                DataStreamerItem<Tuple> item = i % 2 == 0
                        ? DataStreamerItem.of(tuple(i, "new-" + i))
                        : DataStreamerItem.removed(tupleKey(i));

                publisher.submit(item);
            }
        }

        streamerFut.orTimeout(30, TimeUnit.SECONDS).join();

        ArrayList<String> sqlRes = new ArrayList<>(count);
        ResultSet<SqlRow> resultSet = ignite().sql().execute(null, "SELECT name FROM " + TABLE_NAME + " where id >= ? and id < ?", 0, count);
        resultSet.forEachRemaining(row -> sqlRes.add(row.stringValue(0)));

        assertEquals(count / 2, sqlRes.size());

        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                assertEquals("new-" + i, sqlRes.get(i / 2));
            }
        }

        List<Tuple> res = view.getAll(null, IntStream.range(0, count).mapToObj(i -> tupleKey(i)).collect(Collectors.toList()));

        for (int i = 0; i < count; i++) {
            Tuple tuple = res.get(i);

            if (i % 2 == 0) {
                assertEquals("new-" + i, tuple.stringValue("name"));
            } else {
                assertNull(tuple);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"100, false", "100, true", "1000, false", "1000, true"})
    public void testSameItemMultipleUpdatesOrder(int pageSize, boolean existingKey) {
        int id = pageSize + (existingKey ? 1 : 2);
        RecordView<Tuple> view = defaultTable().recordView();

        if (existingKey) {
            view.upsert(null, tuple(id, "old"));
        } else {
            view.delete(null, tupleKey(id));
        }

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            DataStreamerOptions options = DataStreamerOptions.builder().pageSize(pageSize).build();
            streamerFut = view.streamData(publisher, options);

            for (int i = 0; i < 100; i++) {
                publisher.submit(DataStreamerItem.of(tuple(id, "foo-" + i)));
                publisher.submit(DataStreamerItem.removed(tupleKey(id)));
                publisher.submit(DataStreamerItem.of(tuple(id, "bar-" + i)));
            }
        }

        streamerFut.orTimeout(id, TimeUnit.SECONDS).join();

        assertEquals("bar-99", view.get(null, tupleKey(id)).stringValue("name"));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testSameItemInsertRemove(int pageSize) {
        RecordView<Tuple> view = defaultTable().recordView();
        CompletableFuture<Void> streamerFut;
        int key = 333000;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            streamerFut = view.streamData(publisher, DataStreamerOptions.builder().pageSize(pageSize).build());

            publisher.submit(DataStreamerItem.of(tuple(key, "foo")));
            publisher.submit(DataStreamerItem.removed(tupleKey(key)));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNull(view.get(null, tupleKey(key)));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testSameItemInsertRemoveInsertUpdate(int pageSize) {
        RecordView<Tuple> view = defaultTable().recordView();
        CompletableFuture<Void> streamerFut;
        int key = 333001;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            streamerFut = view.streamData(publisher, DataStreamerOptions.builder().pageSize(pageSize).build());

            publisher.submit(DataStreamerItem.of(tuple(key, "foo")));
            publisher.submit(DataStreamerItem.removed(tupleKey(key)));
            publisher.submit(DataStreamerItem.of(tuple(key, "bar")));
            publisher.submit(DataStreamerItem.of(tuple(key, "baz")));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertEquals("baz", view.get(null, tupleKey(key)).stringValue("name"));
    }

    @SuppressWarnings("resource")
    @Test
    public void testSchemaUpdateWhileStreaming() throws InterruptedException {
        IgniteSql sql = ignite().sql();

        String tableName = "testSchemaUpdateWhileStreaming";
        sql.execute(null, "CREATE TABLE " + tableName + "(ID INT NOT NULL PRIMARY KEY)");
        RecordView<Tuple> view = ignite().tables().table(tableName).recordView();

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SimplePublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().pageSize(1).build();
            streamerFut = view.streamData(publisher, options);

            publisher.submit(tupleKey(1));
            waitForKey(view, tupleKey(1));

            sql.execute(null, "ALTER TABLE " + tableName + " ADD COLUMN NAME VARCHAR NOT NULL DEFAULT 'bar'");
            publisher.submit(tupleKey(2));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertEquals("bar", view.get(null, tupleKey(2)).stringValue("name"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testWithReceiverRecordBinaryView(boolean returnResults) {
        testWithReceiver(defaultTable().recordView(), Function.identity(), returnResults);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testWithReceiverKvBinaryView(boolean returnResults) {
        testWithReceiver(defaultTable().keyValueView(), t -> Map.entry(t, t), returnResults);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testWithReceiverRecordPojoView(boolean returnResults) {
        RecordView<PersonPojo> view = defaultTable().recordView(PersonPojo.class);

        testWithReceiver(view, t -> new PersonPojo(t.intValue(0)), returnResults);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testWithReceiverKvPojoView(boolean returnResults) {
        KeyValueView<Integer, PersonValPojo> view = defaultTable().keyValueView(Mapper.of(Integer.class), Mapper.of(PersonValPojo.class));

        testWithReceiver(view, t -> Map.entry(t.intValue(0), new PersonValPojo()), returnResults);
    }

    private static <T> void testWithReceiver(DataStreamerTarget<T> target, Function<Tuple, T> keyFunc, boolean returnResults) {
        CompletableFuture<Void> streamerFut;

        var resultSubscriber = returnResults ? new TestSubscriber<String>() : null;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            streamerFut = target.streamData(
                    publisher,
                    DataStreamerReceiverDescriptor.builder(TestReceiver.class).build(),
                    keyFunc,
                    t -> t.stringValue(1),
                    "arg1",
                    resultSubscriber,
                    DataStreamerOptions.builder().retryLimit(0).build()
            );

            // Same ID goes to the same partition.
            publisher.submit(tuple(1, "val1"));
            publisher.submit(tuple(1, "val2"));
            publisher.submit(tuple(1, "val3"));
        }

        assertThat(streamerFut, willCompleteSuccessfully());

        if (returnResults) {
            assertEquals(1, resultSubscriber.items.size());
            assertEquals("Received: 3 items, arg1 arg", resultSubscriber.items.iterator().next());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testWithReceiverRecordBinaryViewDeprecated(boolean returnResults) {
        testWithReceiverDeprecated(defaultTable().recordView(), Function.identity(), returnResults);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testWithReceiverKvBinaryViewDeprecated(boolean returnResults) {
        testWithReceiverDeprecated(defaultTable().keyValueView(), t -> Map.entry(t, t), returnResults);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testWithReceiverRecordPojoViewDeprecated(boolean returnResults) {
        RecordView<PersonPojo> view = defaultTable().recordView(PersonPojo.class);

        testWithReceiverDeprecated(view, t -> new PersonPojo(t.intValue(0)), returnResults);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testWithReceiverKvPojoViewDeprecated(boolean returnResults) {
        KeyValueView<Integer, PersonValPojo> view = defaultTable().keyValueView(Mapper.of(Integer.class), Mapper.of(PersonValPojo.class));

        testWithReceiverDeprecated(view, t -> Map.entry(t.intValue(0), new PersonValPojo()), returnResults);
    }

    @SuppressWarnings("deprecation")
    private static <T> void testWithReceiverDeprecated(DataStreamerTarget<T> target, Function<Tuple, T> keyFunc, boolean returnResults) {
        CompletableFuture<Void> streamerFut;

        var resultSubscriber = returnResults ? new TestSubscriber<String>() : null;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            streamerFut = target.streamData(
                    publisher,
                    keyFunc,
                    t -> t.stringValue(1),
                    ReceiverDescriptor.builder(TestReceiver.class).build(),
                    resultSubscriber,
                    DataStreamerOptions.builder().retryLimit(0).build(),
                    "arg1"
            );

            // Same ID goes to the same partition.
            publisher.submit(tuple(1, "val1"));
            publisher.submit(tuple(1, "val2"));
            publisher.submit(tuple(1, "val3"));
        }

        assertThat(streamerFut, willCompleteSuccessfully());

        if (returnResults) {
            assertEquals(1, resultSubscriber.items.size());
            assertEquals("Received: 3 items, arg1 arg", resultSubscriber.items.iterator().next());
        }
    }

    @Test
    public void testReceiverIsExecutedOnTargetNode() {
        // Await primary replicas before streaming.
        Table table = defaultTable();
        RecordView<Tuple> view = table.recordView();
        Map<Partition, ClusterNode> primaryReplicas = table.partitionDistribution().primaryReplicasAsync().join();

        CompletableFuture<Void> streamerFut;
        int count = 10;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            streamerFut = view.streamData(
                    publisher,
                    DataStreamerReceiverDescriptor.builder(NodeNameReceiver.class).build(),
                    t -> t,
                    t -> t.intValue(0),
                    null,
                    null,
                    null
            );

            for (int i = 0; i < count; i++) {
                publisher.submit(tupleKey(i));
            }
        }

        assertThat(streamerFut, willCompleteSuccessfully());

        for (int i = 0; i < count; i++) {
            ClusterNode expectedNode = table.partitionDistribution().partitionAsync(tupleKey(i)).thenApply(primaryReplicas::get).join();
            String actualNode = view.get(null, tupleKey(i)).stringValue("name");

            assertEquals(expectedNode.name(), actualNode);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReceiverException(boolean async) {
        CompletableFuture<Void> streamerFut;

        Tuple item = tupleKey(1);

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            streamerFut = defaultTable().recordView().streamData(
                    publisher,
                    DataStreamerReceiverDescriptor.builder(TestReceiver.class).build(),
                    t -> t,
                    t -> "",
                    async ? "throw-async" : "throw",
                    null,
                    DataStreamerOptions.builder().retryLimit(0).pageSize(1).build());

            publisher.submit(item);
        }

        var ex = assertThrows(CompletionException.class, () -> streamerFut.orTimeout(1, TimeUnit.SECONDS).join());
        assertThat(
                ex.getCause().getMessage(),
                containsString("Streamer receiver failed: Job execution failed: java.lang.ArithmeticException: test"));

        DataStreamerException cause = (DataStreamerException) ex.getCause();
        assertEquals(1, cause.failedItems().size());
        assertEquals(item, cause.failedItems().iterator().next());
    }

    @Test
    public void testFailedItems() {
        RecordView<Tuple> view = defaultTable().recordView();

        CompletableFuture<Void> streamerFut;

        var invalidItemsAdded = new ArrayList<DataStreamerItem<Tuple>>();
        String invalidColName = "name1";

        try (var publisher = new DirectPublisher<DataStreamerItem<Tuple>>()) {
            var options = DataStreamerOptions.builder()
                    .pageSize(10)
                    .perPartitionParallelOperations(3)
                    .autoFlushInterval(100)
                    .build();

            streamerFut = view.streamData(publisher, options);

            assertTrue(TestUtils.waitForCondition(() -> publisher.requested() > 0, 5000));

            // Submit valid items.
            for (int i = 0; i < 100; i++) {
                publisher.submit(DataStreamerItem.of(tuple(i, "foo-" + i)));
            }

            assertTrue(TestUtils.waitForCondition(() -> view.contains(null, tupleKey(99)), 5000));

            // Submit invalid items.
            for (int i = 200; i < 300; i++) {
                DataStreamerItem<Tuple> item = DataStreamerItem.of(
                        Tuple.create().set("id", i).set(invalidColName, "bar-" + i),
                        i % 2 == 0 ? DataStreamerOperationType.PUT : DataStreamerOperationType.REMOVE);

                try {
                    publisher.submit(item);
                    invalidItemsAdded.add(item);
                } catch (IllegalStateException e) {
                    assertEquals("Streamer is closed, can't add items.", e.getMessage());
                    break;
                } catch (RuntimeException e) {
                    if (unwrapCause(e) instanceof MarshallerException) {
                        // Item was added but failed on flush.
                        invalidItemsAdded.add(item);
                        break;
                    } else {
                        // Unexpected exception.
                        throw e;
                    }
                }
            }
        }

        var ex = assertThrows(CompletionException.class, () -> streamerFut.orTimeout(1, TimeUnit.SECONDS).join());
        DataStreamerException cause = (DataStreamerException) ex.getCause();
        Set<DataStreamerItem<Tuple>> failedItems = (Set<DataStreamerItem<Tuple>>) cause.failedItems();

        for (DataStreamerItem<Tuple> invalidAddedItem : invalidItemsAdded) {
            assertTrue(failedItems.contains(invalidAddedItem), "failedItems item not found: " + invalidAddedItem.get());
        }

        for (DataStreamerItem<Tuple> failedItem : failedItems) {
            if (failedItem.get().columnIndex(invalidColName) < 0) {
                // Valid item failed to flush, ignore.
                continue;
            }

            assertTrue(invalidItemsAdded.contains(failedItem), "invalidItemsAdded item not found: " + failedItem.get());
        }

        assertThat(invalidItemsAdded.size(), is(greaterThan(10)));
    }

    @Test
    public void testReceiverWithTuples() {
        CompletableFuture<Void> streamerFut;
        var resultSubscriber = new TestSubscriber<Tuple>();

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            // Tuple argument.
            Tuple receiverArg = Tuple.create().set("arg1", "val1").set("arg2", 2);

            streamerFut = defaultTable().recordView().streamData(
                    publisher,
                    DataStreamerReceiverDescriptor.builder(TupleReceiver.class).build(),
                    Function.identity(),
                    Function.identity(),
                    receiverArg,
                    resultSubscriber,
                    null
            );

            // Tuple payload.
            publisher.submit(tuple(1, "foo1"));
            publisher.submit(tuple(2, "foo2"));
        }

        assertThat(streamerFut, willCompleteSuccessfully());
        assertEquals(2, resultSubscriber.items.size());

        // Tuple results.
        for (Tuple item : resultSubscriber.items) {
            assertEquals("foo" + item.intValue(0), item.stringValue(1));
            assertEquals("val1", item.stringValue("arg1"));
            assertEquals(2, item.intValue("arg2"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReceiverTupleRoundTripWithAllColumnTypes(boolean asArg) {
        Tuple tuple = Tuple.create()
                .set("bool", true)
                .set("byte", (byte) 1)
                .set("short", (short) 2)
                .set("int", 3)
                .set("long", 4L)
                .set("float", 5.5f)
                .set("double", 6.6)
                .set("decimal", BigDecimal.valueOf(1, 1000))
                .set("date", LocalDate.of(2021, 1, 1))
                .set("time", LocalTime.of(1, 2, 3))
                .set("datetime", LocalDateTime.of(2000, 1, 2, 3, 4, 5))
                .set("uuid", new UUID(1, 2))
                .set("string", "foo")
                .set("binary", new byte[] {1, 2, 3})
                .set("period", Period.ofMonths(3))
                .set("duration", Duration.ofDays(4));

        Tuple resTuple = receiverTupleRoundTrip(tuple, asArg);

        for (int i = 0; i < tuple.columnCount(); i++) {
            Object origVal = tuple.value(i);
            Object resVal = resTuple.value(i);

            if (origVal instanceof byte[]) {
                assertArrayEquals((byte[]) origVal, (byte[]) resVal);
            } else {
                assertEquals(origVal, resVal);
            }
        }
    }

    @Test
    public void testReceiverNestedTupleRoundTrip() {
        Tuple tuple = Tuple.create()
                .set("int", 1)
                .set("inner", Tuple.create()
                        .set("string", "foo")
                        .set("inner2", Tuple.create().set("int", 2)));

        Tuple resTuple = receiverTupleRoundTrip(tuple, false);
        assertEquals(1, resTuple.intValue("int"));

        Tuple resTupleInner = resTuple.value("inner");
        assertEquals("foo", resTupleInner.stringValue("string"));

        Tuple resTupleInner2 = resTupleInner.value("inner2");
        assertEquals(2, resTupleInner2.intValue("int"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"arg1", ""})
    public void testMarshallingReceiver(String arg) {
        // Check that null arg works.
        arg = arg.isEmpty() ? null : arg;

        DataStreamerReceiverDescriptor<String, String, String> desc = DataStreamerReceiverDescriptor
                .builder(MarshallingReceiver.class)
                .payloadMarshaller(new StringSuffixMarshaller())
                .argumentMarshaller(new StringSuffixMarshaller())
                .resultMarshaller(new StringSuffixMarshaller())
                .build();

        CompletableFuture<Void> streamerFut;
        var resultSubscriber = new TestSubscriber<String>();

        try (var publisher = new SubmissionPublisher<String>()) {
            streamerFut = defaultTable().recordView().streamData(
                    publisher,
                    desc,
                    x -> Tuple.create().set("id", 1),
                    Function.identity(),
                    arg,
                    resultSubscriber,
                    null
            );

            publisher.submit("val1");
            publisher.submit("val2");
        }

        assertThat(streamerFut, willCompleteSuccessfully());
        assertEquals(2, resultSubscriber.items.size());

        String expected = "received[arg=" + arg + ":beforeMarshal:afterUnmarshal,val=val1:beforeMarshal:afterUnmarshal]"
                + ":beforeMarshal:afterUnmarshal";

        assertEquals(expected, resultSubscriber.items.get(0));
    }

    @Test
    public void testReceiverMarshallerMismatch() {
        DataStreamerReceiverDescriptor<String, String, String> desc = DataStreamerReceiverDescriptor
                .builder(MarshallingReceiver.class)
                .build();

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<String>()) {
            streamerFut = defaultTable().recordView().streamData(
                    publisher,
                    desc,
                    x -> Tuple.create().set("id", 1),
                    Function.identity(),
                    "arg",
                    null,
                    null
            );

            publisher.submit("val1");
        }

        var ex = assertThrows(CompletionException.class, () -> streamerFut.orTimeout(10, TimeUnit.SECONDS).join());
        DataStreamerException dsEx = (DataStreamerException) ex.getCause();

        assertThat(dsEx.getMessage(), containsString(
                "Marshaller is defined in the DataStreamerReceiver implementation, "
                        + "expected argument type: `byte[]`, actual: `class java.lang.String`. "
                        + "Ensure that DataStreamerReceiverDescriptor marshallers match DataStreamerReceiver marshallers."));

        assertEquals("IGN-COMPUTE-13", dsEx.codeAsString());
    }

    @Test
    public void testReceiverResultsObservedImmediately() {
        int count = 10_000;

        RecordView<Tuple> view = defaultTable().recordView();
        CompletableFuture<Void> streamerFut;

        DataStreamerReceiverDescriptor<Tuple, Tuple, Void> desc = DataStreamerReceiverDescriptor
                .builder(UpsertReceiver.class)
                .build();

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            streamerFut = view.streamData(publisher, desc, Function.identity(), Function.identity(), null, null, null);

            for (int i = 0; i < count; i++) {
                Tuple tuple = Tuple.create()
                        .set("id", i)
                        .set("name", "name-" + i);

                publisher.submit(tuple);
            }
        }

        assertThat(streamerFut, willCompleteSuccessfully());

        // Check that receiver execution results are observed immediately after the streaming completes.
        // This is achieved by propagating correct hybridTimestamp to the client after every receiver execution.
        int resCount = 0;

        try (Cursor<Tuple> cursor = view.query(null, null)) {
            while (cursor.hasNext()) {
                resCount++;
                Tuple item = cursor.next();

                assertEquals("name-" + item.intValue("id"), item.stringValue("name"));
            }
        }

        assertEquals(count, resCount);
    }

    private Tuple receiverTupleRoundTrip(Tuple tuple, boolean asArg) {
        CompletableFuture<Void> streamerFut;
        var resultSubscriber = new TestSubscriber<Tuple>();

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            Tuple receiverArg = asArg ? tuple : Tuple.create();

            streamerFut = defaultTable().recordView().streamData(
                    publisher,
                    DataStreamerReceiverDescriptor.builder(TupleReceiver.class).build(),
                    Function.identity(),
                    Function.identity(),
                    receiverArg,
                    resultSubscriber,
                    null
            );

            publisher.submit(asArg ? Tuple.create() : tuple);
        }

        assertThat(streamerFut, willCompleteSuccessfully());
        assertEquals(1, resultSubscriber.items.size());

        return resultSubscriber.items.get(0);
    }

    private void waitForKey(RecordView<Tuple> view, Tuple key) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            var tx = ignite().transactions().begin(new TransactionOptions().readOnly(true));

            try {
                return view.get(tx, key) != null;
            } finally {
                tx.rollback();
            }
        }, 50, 5000));
    }

    private Table defaultTable() {
        return ignite().tables().table(TABLE_NAME);
    }

    private Table compositeKeyTable() {
        return ignite().tables().table(TABLE_NAME_COMPOSITE_KEY);
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

    private static Tuple compositeKeyTuple(int id) {
        return Tuple.create()
                .set("name", "name" + id)
                .set("data", "data" + id)
                .set("uniqueId", "uniqueId" + id)
                .set("foo", "foo" + id);
    }

    private static Tuple compositeKeyTupleKey(int id) {
        return Tuple.create()
                .set("name", "name" + id)
                .set("uniqueId", "uniqueId" + id);
    }

    private static Tuple compositeKeyTupleVal(int id) {
        return Tuple.create()
                .set("data", "data" + id)
                .set("foo", "foo" + id);
    }

    @SuppressWarnings("unused")
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

    @SuppressWarnings("unused")
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

    @SuppressWarnings("unused")
    private static class CompositeKeyPojo {
        String name;
        String data;
        String uniqueId;
        String foo;

        @SuppressWarnings("unused") // Required by serializer.
        private CompositeKeyPojo() {
            // No-op.
        }

        CompositeKeyPojo(int id) {
            this.name = "name" + id;
            this.uniqueId = "uniqueId" + id;
        }

        CompositeKeyPojo(int id, String data, String foo) {
            this.name = "name" + id;
            this.data = data;
            this.uniqueId = "uniqueId" + id;
            this.foo = foo;
        }
    }

    @SuppressWarnings("unused")
    private static class CompositeKeyKeyPojo {
        String name;
        String uniqueId;

        @SuppressWarnings("unused") // Required by serializer.
        private CompositeKeyKeyPojo() {
            // No-op.
        }

        CompositeKeyKeyPojo(int id) {
            this.name = "name" + id;
            this.uniqueId = "uniqueId" + id;
        }
    }

    @SuppressWarnings("unused")
    private static class CompositeKeyValPojo {
        String data;
        String foo;

        @SuppressWarnings("unused") // Required by serializer.
        private CompositeKeyValPojo() {
            // No-op.
        }

        CompositeKeyValPojo(int id) {
            this.data = "data" + id;
            this.foo = "foo" + id;
        }
    }

    private static class TestReceiver implements DataStreamerReceiver<String, Object, String> {
        @Override
        public CompletableFuture<List<String>> receive(List<String> page, DataStreamerReceiverContext ctx, Object arg) {
            if ("throw".equals(arg)) {
                throw new ArithmeticException("test");
            }

            if ("throw-async".equals(arg)) {
                return CompletableFuture.failedFuture(new ArithmeticException("test"));
            }

            assertEquals(3, page.size());
            assertEquals("val1", page.get(0));
            assertEquals("val2", page.get(1));
            assertEquals("val3", page.get(2));

            assertNotNull(ctx.ignite().tables().table(TABLE_NAME));

            assertEquals("arg1", arg);

            return CompletableFuture.completedFuture(List.of("Received: " + page.size() + " items, " + arg + " arg"));
        }
    }

    private static class NodeNameReceiver implements DataStreamerReceiver<Integer, Object, Void> {
        @Override
        public @Nullable CompletableFuture<List<Void>> receive(List<Integer> page, DataStreamerReceiverContext ctx, Object arg) {
            var nodeName = ctx.ignite().name();
            RecordView<Tuple> view = ctx.ignite().tables().table(TABLE_NAME).recordView();

            for (Integer id : page) {
                view.upsert(null, tuple(id, nodeName));
            }

            return null;
        }
    }

    private static class TestSubscriber<T> implements Subscriber<T> {
        List<T> items = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onSubscribe(Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            items.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onComplete() {
        }
    }

    private static class TupleReceiver implements DataStreamerReceiver<Tuple, Tuple, Tuple> {
        @Override
        public @Nullable CompletableFuture<List<Tuple>> receive(List<Tuple> page, DataStreamerReceiverContext ctx, @Nullable Tuple arg) {
            // Add all columns from arg to each tuple.
            for (Tuple t : page) {
                for (int colIdx = 0; colIdx < arg.columnCount(); colIdx++) {
                    t.set(arg.columnName(colIdx), arg.value(colIdx));
                }
            }

            return CompletableFuture.completedFuture(page);
        }
    }

    private static class StringSuffixMarshaller implements ByteArrayMarshaller<String> {
        @Override
        public byte @Nullable [] marshal(@Nullable String object) {
            return ByteArrayMarshaller.super.marshal(object + ":beforeMarshal");
        }

        @Override
        public @Nullable String unmarshal(byte @Nullable [] raw) {
            return ByteArrayMarshaller.super.unmarshal(raw) + ":afterUnmarshal";
        }
    }

    private static class MarshallingReceiver implements DataStreamerReceiver<String, String, String> {
        @Override
        public @Nullable CompletableFuture<List<String>> receive(List<String> page, DataStreamerReceiverContext ctx, @Nullable String arg) {
            var results = page.stream()
                    .map(s -> "received[arg=" + arg + ",val=" + s + "]")
                    .collect(Collectors.toList());

            return CompletableFuture.completedFuture(results);
        }

        @Override
        public @Nullable Marshaller<String, byte[]> payloadMarshaller() {
            return new StringSuffixMarshaller();
        }

        @Override
        public @Nullable Marshaller<String, byte[]> argumentMarshaller() {
            return new StringSuffixMarshaller();
        }

        @Override
        public @Nullable Marshaller<String, byte[]> resultMarshaller() {
            return new StringSuffixMarshaller();
        }
    }

    private static class UpsertReceiver implements DataStreamerReceiver<Tuple, Tuple, Void> {
        @Override
        public @Nullable CompletableFuture<List<Void>> receive(List<Tuple> page, DataStreamerReceiverContext ctx, @Nullable Tuple arg) {
            RecordView<Tuple> view = ctx.ignite().tables().table(TABLE_NAME).recordView();

            return view.upsertAllAsync(null, page).thenApply(x -> null);
        }
    }
}
