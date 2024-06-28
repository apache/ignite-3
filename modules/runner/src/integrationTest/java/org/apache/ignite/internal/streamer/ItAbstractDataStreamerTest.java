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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
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
    public void testAutoFlushByTimer() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SimplePublisher<Tuple>()) {
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

        try (var publisher = new SimplePublisher<Tuple>()) {
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

        try (var publisher = new SimplePublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().build();
            streamerFut = view.streamData(publisher, options);

            var tuple = Tuple.create();

            publisher.submit(tuple);
        }

        var ex = assertThrows(CompletionException.class, () -> streamerFut.orTimeout(1, TimeUnit.SECONDS).join());
        assertEquals("Missed key column: ID", ex.getCause().getMessage());
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
    public void testWithReceiver(boolean returnResults) {
        CompletableFuture<Void> streamerFut;

        var resultSubscriber = returnResults ? new TestSubscriber<String>() : null;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            streamerFut = defaultTable().recordView().streamData(
                    publisher,
                    t -> t,
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
    public void testReceivedIsExecutedOnTargetNode() {
        // Await primary replicas before streaming.
        Table table = defaultTable();
        RecordView<Tuple> view = table.recordView();
        Map<Partition, ClusterNode> primaryReplicas = table.partitionManager().primaryReplicasAsync().join();

        CompletableFuture<Void> streamerFut;
        int count = 10;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            streamerFut = view.streamData(
                    publisher,
                    t -> t,
                    t -> t.intValue(0),
                    ReceiverDescriptor.builder(NodeNameReceiver.class).build(),
                    null, null, null
            );

            for (int i = 0; i < count; i++) {
                publisher.submit(tupleKey(i));
            }
        }

        assertThat(streamerFut, willCompleteSuccessfully());

        for (int i = 0; i < count; i++) {
            var expectedNode = table.partitionManager().partitionAsync(tupleKey(i)).thenApply(primaryReplicas::get).join();
            var actualNode = view.get(null, tupleKey(i)).stringValue("name");

            assertEquals(expectedNode.name(), actualNode);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReceiverException(boolean async) {
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            streamerFut = defaultTable().recordView().streamData(
                    publisher,
                    t -> t,
                    t -> 0,
                    ReceiverDescriptor.builder(TestReceiver.class).build(),
                    null,
                    DataStreamerOptions.builder().retryLimit(0).pageSize(1).build(),
                    async ? "throw-async" : "throw");

            publisher.submit(tupleKey(1));
        }

        var ex = assertThrows(CompletionException.class, () -> streamerFut.orTimeout(1, TimeUnit.SECONDS).join());
        assertEquals(
                "Streamer receiver failed: Job execution failed: java.lang.ArithmeticException: test",
                ex.getCause().getMessage());
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

    @SuppressWarnings("resource")
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

    @SuppressWarnings("resource")
    private static class NodeNameReceiver implements DataStreamerReceiver<Integer, Object, Void> {
        @Override
        public @Nullable CompletableFuture<List<Void>> receive(List<Integer> page, DataStreamerReceiverContext ctx, Object args) {
            var nodeName = ctx.ignite().name();
            RecordView<Tuple> view = ctx.ignite().tables().table(TABLE_NAME).recordView();

            for (Integer id : page) {
                view.upsert(null, tuple(id, nodeName));
            }

            return null;
        }
    }

    private static class TestSubscriber<T> implements Subscriber<T> {
        Set<T> items = Collections.synchronizedSet(new HashSet<>());

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
}
