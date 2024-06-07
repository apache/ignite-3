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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.streamer.SimplePublisher;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Data streamer test.
 */
@SuppressWarnings("DataFlowIssue")
public class DataStreamerTest extends AbstractClientTableTest {
    private IgniteClient client2;

    private TestServer testServer2;

    @AfterEach
    public void afterEach() throws Exception {
        closeAll(client2, testServer2);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testBasicStreamingRecordBinaryView(int batchSize) {
        RecordView<Tuple> view = defaultTable().recordView();

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SimplePublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().pageSize(batchSize).build();
            streamerFut = view.streamData(publisher, options);

            publisher.submit(tuple(1L, "foo"));
            publisher.submit(tuple(2L, "bar"));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNotNull(view.get(null, tupleKey(1L)));
        assertNotNull(view.get(null, tupleKey(2L)));
        assertNull(view.get(null, tupleKey(3L)));

        assertEquals("bar", view.get(null, tupleKey(2L)).stringValue("name"));
    }

    @Test
    public void testBasicStreamingRecordPojoView() {
        RecordView<PersonPojo> view = defaultTable().recordView(PersonPojo.class);
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SimplePublisher<PersonPojo>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(new PersonPojo(1L, "foo"));
            publisher.submit(new PersonPojo(2L, "bar"));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, new PersonPojo(2L)).name);
    }

    @Test
    public void testBasicStreamingKvBinaryView() {
        KeyValueView<Tuple, Tuple> view = defaultTable().keyValueView();
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SimplePublisher<Entry<Tuple, Tuple>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(Map.entry(tupleKey(1L), tupleVal("foo")));
            publisher.submit(Map.entry(tupleKey(2L), tupleVal("bar")));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, tupleKey(2L)).stringValue("name"));
    }

    @Test
    public void testBasicStreamingKvPojoView() {
        KeyValueView<Long, PersonValPojo> view = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(PersonValPojo.class));
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SimplePublisher<Entry<Long, PersonValPojo>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(Map.entry(1L, new PersonValPojo("foo")));
            publisher.submit(Map.entry(2L, new PersonValPojo("bar")));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, 2L).name);
    }

    @Test
    public void testAutoFlushByTimer() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();

        try (var publisher = new SimplePublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().autoFlushFrequency(100).build();
            view.streamData(publisher, options);

            publisher.submit(tuple(1L, "foo"));
            assertTrue(waitForCondition(() -> view.get(null, tupleKey(1L)) != null, 1000));
        }
    }

    @Test
    public void testAutoFlushDisabled() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();

        try (var publisher = new SimplePublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().autoFlushFrequency(-1).build();
            view.streamData(publisher, options);

            publisher.submit(tuple(1L, "foo"));
            assertFalse(waitForCondition(() -> view.get(null, tupleKey(1L)) != null, 1000));
        }
    }

    @Test
    public void testBackPressure() {
        Function<Integer, Integer> responseDelay = idx -> idx > 5 ? 500 : 0;
        var ignite2 = startTestServer2(idx -> false, responseDelay);

        var port = testServer2.port();

        client2 = IgniteClient.builder().addresses("localhost:" + port).build();
        RecordView<Tuple> view = defaultTableView(ignite2, client2);

        var bufferSize = 2;
        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>(ForkJoinPool.commonPool(), bufferSize)) {
            var options = DataStreamerOptions.builder()
                    .pageSize(bufferSize)
                    .perPartitionParallelOperations(1)
                    .build();

            var streamerFut = view.streamData(publisher, options);

            // Stream 20 items (5 per partition) while buffer capacity is 2 to trigger back pressure.
            var submitFut = CompletableFuture.runAsync(() -> {
                for (long i = 0; i < 20; i++) {
                    publisher.submit(DataStreamerItem.of(tuple(i, "foo_" + i)));
                }
            });

            // Due to `responseDelay` above, `publisher.submit` is blocking when buffer is full => submitFut can't complete in 200 ms.
            assertThrows(TimeoutException.class, () -> submitFut.get(200, TimeUnit.MILLISECONDS));
            assertFalse(streamerFut.isDone());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testManyItemsWithDisconnectAndRetry(boolean withReceiver) throws Exception {
        // Drop connection on every 5th request.
        Function<Integer, Boolean> shouldDropConnection = idx -> idx % 5 == 4;
        var ignite2 = startTestServer2(shouldDropConnection, idx -> 0);

        // Streamer has it's own retry policy, so we can disable retries on the client.
        Builder builder = IgniteClient.builder()
                .addresses("localhost:" + testServer2.port())
                .retryPolicy(new RetryLimitPolicy().retryLimit(0))
                .reconnectThrottlingPeriod(0)
                .loggerFactory(new ConsoleLoggerFactory("client-2"));

        client2 = builder.build();
        RecordView<Tuple> view = defaultTableView(ignite2, client2);
        CompletableFuture<Void> streamFut;

        try (var publisher = new SimplePublisher<Tuple>()) {
            var options = DataStreamerOptions.builder()
                    .pageSize(2)
                    .perPartitionParallelOperations(4)
                    .build();

            streamFut = withReceiver
                    ? view.streamData(
                        publisher,
                        options,
                        DataStreamerItem::get,
                        t -> t.get().longValue("id"),
                        null,
                        List.of(),
                        TestUpsertReceiver.class.getName())
                    : view.streamData(publisher, options);

            for (long i = 0; i < 1000; i++) {
                publisher.submit(tuple(i, "foo_" + i));
            }
        }

        streamFut.get(5, TimeUnit.SECONDS);

        for (long i = 0; i < 100; i++) {
            assertNotNull(view.get(null, tupleKey(i)), "Failed to get tuple: " + i);
        }
    }

    @Test
    public void testRetryLimitExhausted() {
        Function<Integer, Boolean> shouldDropConnection = idx -> idx > 10;
        var ignite2 = startTestServer2(shouldDropConnection, idx -> 0);

        var logger = new TestLoggerFactory("client-2");

        Builder builder = IgniteClient.builder()
                .addresses("localhost:" + testServer2.port())
                .loggerFactory(logger)
                .retryPolicy(new RetryLimitPolicy().retryLimit(3));

        client2 = builder.build();
        RecordView<Tuple> view = defaultTableView(ignite2, client2);
        CompletableFuture<Void> streamFut;

        try (var publisher = new SimplePublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().pageSize(2).retryLimit(3).build();
            streamFut = view.streamData(publisher, options);

            for (long i = 0; i < 100; i++) {
                publisher.submit(tuple(i, "foo_" + i));
            }
        }

        assertThrows(ExecutionException.class, () -> streamFut.get(5, TimeUnit.SECONDS));
        logger.assertLogContains("Not retrying operation [opCode=62, opType=STREAMER_BATCH_SEND, attempt=3");
        logger.assertLogContains("Failed to send batch to partition");
    }

    @Test
    public void testAssignmentRefreshErrorClosesStreamer() {
        // Drop connection before we can retrieve partition assignment.
        Function<Integer, Boolean> shouldDropConnection = idx -> idx > 3;
        var ignite2 = startTestServer2(shouldDropConnection, idx -> 0);

        var logger = new TestLoggerFactory("client-2");

        Builder builder = IgniteClient.builder()
                .addresses("localhost:" + testServer2.port())
                .loggerFactory(logger)
                .retryPolicy(new RetryLimitPolicy().retryLimit(1));

        client2 = builder.build();
        RecordView<Tuple> view = defaultTableView(ignite2, client2);
        CompletableFuture<Void> streamFut;

        try (var publisher = new SimplePublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().retryLimit(0).build();
            streamFut = view.streamData(publisher, options);
            publisher.submit(tuple(1L, "foo"));
        }

        assertThrows(ExecutionException.class, () -> streamFut.get(5, TimeUnit.SECONDS));
        logger.assertLogContains("Failed to refresh schemas and partition assignment");
    }

    @Test
    public void testAddUpdateRemove() {
        RecordView<Tuple> view = defaultTable().recordView();
        view.delete(null, tupleKey(1L));
        view.upsert(null, tuple(2L, "foo"));
        view.upsert(null, tuple(3L, "bar"));

        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            streamerFut = view.streamData(publisher, null);

            // Add.
            publisher.submit(DataStreamerItem.of(tuple(1L, "foo")));

            // Update.
            publisher.submit(DataStreamerItem.of(tuple(2L, "bar2")));

            // Remove.
            publisher.submit(DataStreamerItem.removed(tupleKey(3L)));
            publisher.submit(DataStreamerItem.removed(tuple(4L, "x")));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNotNull(view.get(null, tupleKey(1L)));
        assertNotNull(view.get(null, tupleKey(2L)));
        assertNull(view.get(null, tupleKey(3L)));

        assertEquals("bar2", view.get(null, tupleKey(2L)).stringValue("name"));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testBasicStreamingWithReceiverRecordBinaryView(int batchSize) {
        RecordView<Tuple> view = defaultTable().recordView();
        CompletableFuture<Void> streamerFut;
        int count = 3;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().pageSize(batchSize).build();
            streamerFut = view.streamData(
                    publisher,
                    options,
                    t -> t,
                    t -> t.longValue("id"),
                    null,
                    new ArrayList<>(),
                    TestReceiver.class.getName(),
                    "arg");

            for (long i = 0; i < count; i++) {
                publisher.submit(tuple(i));
            }
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        for (long i = 0; i < count; i++) {
            assertEquals("recv_arg_" + i, view.get(null, tupleKey(i)).stringValue("name"));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testBasicStreamingWithReceiverAndSubscriberRecordBinaryView(int batchSize) {
        RecordView<Tuple> view = defaultTable().recordView();
        CompletableFuture<Void> streamerFut;
        int count = 3;

        var resultSubscriber = new TestSubscriber<String>();

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().pageSize(batchSize).build();
            streamerFut = view.streamData(
                    publisher,
                    options,
                    t -> t,
                    t -> t.longValue("id"),
                    resultSubscriber,
                    new ArrayList<>(),
                    TestReceiver.class.getName(),
                    "arg",
                    "returnResults");

            for (long i = 0; i < count; i++) {
                publisher.submit(tuple(i));
            }
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        assertTrue(resultSubscriber.completed.get());
        assertNull(resultSubscriber.error.get());
        assertEquals(count, resultSubscriber.items.size());

        for (long i = 0; i < count; i++) {
            String expectedName = "recv_arg_" + i;
            assertEquals(expectedName, view.get(null, tupleKey(i)).stringValue("name"));

            assertTrue(resultSubscriber.items.contains(expectedName));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBasicStreamingWithReceiverRecordPojoView(boolean withSubscriber) {
        RecordView<PersonPojo> view = defaultTable().recordView(PersonPojo.class);
        CompletableFuture<Void> streamerFut;
        int count = 3;

        var resultSubscriber = withSubscriber ? new TestSubscriber<String>() : null;

        try (var publisher = new SubmissionPublisher<PersonPojo>()) {
            streamerFut = view.streamData(
                    publisher,
                    null,
                    t -> t,
                    t -> t.id,
                    resultSubscriber,
                    new ArrayList<>(),
                    TestReceiver.class.getName(),
                    "arg",
                    withSubscriber ? "returnResults" : "noResults");

            for (long i = 0; i < count; i++) {
                publisher.submit(new PersonPojo(i));
            }
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        if (withSubscriber) {
            assertTrue(resultSubscriber.completed.get());
            assertNull(resultSubscriber.error.get());
            assertEquals(count, resultSubscriber.items.size());
        }

        for (long i = 0; i < count; i++) {
            assertEquals("recv_arg_" + i, view.get(null, new PersonPojo(i)).name);

            if (withSubscriber) {
                assertTrue(resultSubscriber.items.contains("recv_arg_" + i));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBasicStreamingWithReceiverKvBinaryView(boolean withSubscriber) {
        KeyValueView<Tuple, Tuple> view = defaultTable().keyValueView();
        CompletableFuture<Void> streamerFut;
        int count = 3;

        var resultSubscriber = withSubscriber ? new TestSubscriber<String>() : null;

        try (var publisher = new SubmissionPublisher<Entry<Tuple, Tuple>>()) {
            streamerFut = view.streamData(
                    publisher,
                    null,
                    t -> t,
                    t -> t.getKey().longValue(0),
                    resultSubscriber,
                    new ArrayList<>(),
                    TestReceiver.class.getName(),
                    "arg");

            for (long i = 0; i < count; i++) {
                publisher.submit(Map.entry(tupleKey(i), tupleVal("foo")));
            }
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        if (withSubscriber) {
            assertTrue(resultSubscriber.completed.get());
            assertNull(resultSubscriber.error.get());
            assertEquals(count, resultSubscriber.items.size());
        }

        for (long i = 0; i < count; i++) {
            assertEquals("recv_arg_" + i, view.get(null, tupleKey(i)).stringValue(0));

            if (withSubscriber) {
                assertTrue(resultSubscriber.items.contains("recv_arg_" + i));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBasicStreamingWithReceiverKvPojoView(boolean withSubscriber) {
        KeyValueView<Long, PersonValPojo> view = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(PersonValPojo.class));
        CompletableFuture<Void> streamerFut;
        int count = 3;

        var resultSubscriber = withSubscriber ? new TestSubscriber<String>() : null;

        try (var publisher = new SubmissionPublisher<Entry<Long, PersonValPojo>>()) {
            streamerFut = view.streamData(
                    publisher,
                    null,
                    t -> t,
                    Entry::getKey,
                    resultSubscriber,
                    new ArrayList<>(),
                    TestReceiver.class.getName(),
                    "arg");

            for (long i = 0; i < count; i++) {
                publisher.submit(Map.entry(i, new PersonValPojo("foo")));
            }
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();

        if (withSubscriber) {
            assertTrue(resultSubscriber.completed.get());
            assertNull(resultSubscriber.error.get());
            assertEquals(count, resultSubscriber.items.size());
        }

        for (long i = 0; i < count; i++) {
            assertEquals("recv_arg_" + i, view.get(null, i).name);

            if (withSubscriber) {
                assertTrue(resultSubscriber.items.contains("recv_arg_" + i));
            }
        }
    }

    @Test
    public void testReceiverRequiresSameItemTypes() {
        RecordView<Tuple> view = defaultTable().recordView();
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Long>()) {
            streamerFut = view.streamData(
                    publisher,
                    null,
                    id -> tuple(0L),
                    id -> id == 1L ? 1 : "2",
                    null,
                    new ArrayList<>(),
                    TestReceiver.class.getName(),
                    "arg");

            publisher.submit(1L);
            publisher.submit(2L);
        }

        ExecutionException e = assertThrows(ExecutionException.class, () -> streamerFut.get(1, TimeUnit.SECONDS));
        assertTrue(e.getMessage().contains("All items must have the same type. "
                + "First item: class java.lang.Integer, "
                + "current item: class java.lang.String"));
    }

    private static RecordView<Tuple> defaultTableView(FakeIgnite server, IgniteClient client) {
        ((FakeIgniteTables) server.tables()).createTable(DEFAULT_TABLE);

        return client.tables().table(DEFAULT_TABLE).recordView();
    }

    private FakeIgnite startTestServer2(Function<Integer, Boolean> shouldDropConnection, Function<Integer, Integer> responseDelay) {
        var ignite2 = new FakeIgnite("server-2");
        testServer2 = new TestServer(10_000, ignite2, shouldDropConnection, responseDelay, null, UUID.randomUUID(), null, null);

        return ignite2;
    }

    private static class TestReceiver implements DataStreamerReceiver<Long, String> {
        @Override
        public CompletableFuture<List<String>> receive(List<Long> page, DataStreamerReceiverContext ctx, Object... args) {
            // noinspection resource
            RecordView<Tuple> view = ctx.ignite().tables().table(DEFAULT_TABLE).recordView();
            List<String> res = new ArrayList<>(page.size());

            for (Long id : page) {
                String name = "recv_" + args[0] + "_" + id;
                view.upsert(null, tuple(id, name));
                res.add(name);
            }

            boolean returnResults = args.length > 1 && "returnResults".equals(args[1]);
            return CompletableFuture.completedFuture(returnResults ? res : null);
        }
    }

    private static class TestUpsertReceiver implements DataStreamerReceiver<Long, Void> {
        @Override
        @Nullable
        public CompletableFuture<List<Void>> receive(List<Long> page, DataStreamerReceiverContext ctx, Object... args) {
            // noinspection resource
            RecordView<Tuple> view = ctx.ignite().tables().table(DEFAULT_TABLE).recordView();

            for (Long id : page) {
                view.upsert(null, tuple(id, "foo_" + id));
            }

            return null;
        }
    }

    private static class TestSubscriber<T> implements Subscriber<T> {
        Set<T> items = Collections.synchronizedSet(new HashSet<>());
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean();

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
            error.set(throwable);
        }

        @Override
        public void onComplete() {
            completed.set(true);
        }
    }
}
