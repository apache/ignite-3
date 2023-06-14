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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Data streamer test.
 */
public class DataStreamerTest extends AbstractClientTableTest {
    private IgniteClient client2;

    private TestServer testServer2;

    @AfterEach
    public void afterEach() throws Exception {
        if (client2 != null) {
            client2.close();
        }

        if (testServer2 != null) {
            testServer2.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testBasicStreamingRecordBinaryView(int batchSize) {
        RecordView<Tuple> view = defaultTable().recordView();

        var publisher = new SubmissionPublisher<Tuple>();
        var options = DataStreamerOptions.builder().batchSize(batchSize).build();
        CompletableFuture<Void> streamerFut = view.streamData(publisher, options);

        publisher.submit(tuple(1L, "foo"));
        publisher.submit(tuple(2L, "bar"));

        publisher.close();
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

        try (var publisher = new SubmissionPublisher<PersonPojo>()) {
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

        try (var publisher = new SubmissionPublisher<Map.Entry<Tuple, Tuple>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(Map.entry(tupleKey(1L), tuple(1L, "foo")));
            publisher.submit(Map.entry(tupleKey(2L), tuple(2L, "bar")));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, tupleKey(2L)).stringValue("name"));
    }

    @Test
    public void testBasicStreamingKvPojoView() {
        KeyValueView<Long, PersonPojo> view = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(PersonPojo.class));
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<Map.Entry<Long, PersonPojo>>()) {
            streamerFut = view.streamData(publisher, null);

            publisher.submit(Map.entry(1L, new PersonPojo(1L, "foo")));
            publisher.submit(Map.entry(2L, new PersonPojo(2L, "bar")));
        }

        streamerFut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, 2L).name);
    }

    @Test
    public void testAutoFlushByTimer() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();

        var publisher = new SubmissionPublisher<Tuple>();
        var options = DataStreamerOptions.builder().autoFlushFrequency(100).build();
        view.streamData(publisher, options);

        publisher.submit(tuple(1L, "foo"));
        assertTrue(waitForCondition(() -> view.get(null, tupleKey(1L)) != null, 1000));
    }

    @Test
    public void testAutoFlushDisabled() throws InterruptedException {
        RecordView<Tuple> view = this.defaultTable().recordView();

        var publisher = new SubmissionPublisher<Tuple>();
        var options = DataStreamerOptions.builder().autoFlushFrequency(-1).build();
        view.streamData(publisher, options);

        publisher.submit(tuple(1L, "foo"));
        assertFalse(waitForCondition(() -> view.get(null, tupleKey(1L)) != null, 1000));
    }

    @Test
    public void testBackPressure() {
        Function<Integer, Integer> responseDelay = idx -> idx > 5 ? 500 : 0;
        var ignite2 = startTestServer2(idx -> false, responseDelay);

        var port = testServer2.port();

        client2 = IgniteClient.builder().addresses("localhost:" + port).build();
        RecordView<Tuple> view = defaultTableView(ignite2, client2);

        var bufferSize = 2;
        var publisher = new SubmissionPublisher<Tuple>(ForkJoinPool.commonPool(), bufferSize);

        var options = DataStreamerOptions.builder()
                .batchSize(bufferSize)
                .perNodeParallelOperations(1)
                .build();

        var streamerFut = view.streamData(publisher, options);

        // Stream 10 items while buffer capacity is 2 to trigger back pressure.
        var submitFut = CompletableFuture.runAsync(() -> {
            for (long i = 0; i < 10; i++) {
                publisher.submit(tuple(i, "foo_" + i));
            }
        });

        // Due to `responseDelay` above, `publisher.submit` is blocking when buffer is full => submitFut can't complete in 200 ms.
        assertThrows(TimeoutException.class, () -> submitFut.get(200, TimeUnit.MILLISECONDS));
        assertFalse(streamerFut.isDone());
    }

    @Test
    public void testManyItemsWithDisconnectAndRetry() throws Exception {
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

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().batchSize(2).build();
            streamFut = view.streamData(publisher, options);

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

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().batchSize(2).retryLimit(3).build();
            streamFut = view.streamData(publisher, options);

            for (long i = 0; i < 100; i++) {
                publisher.submit(tuple(i, "foo_" + i));
            }
        }

        assertThrows(ExecutionException.class, () -> streamFut.get(5, TimeUnit.SECONDS));
        logger.assertLogContains("Not retrying operation [opCode=13, opType=TUPLE_UPSERT_ALL, attempt=3");
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

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().retryLimit(0).build();
            streamFut = view.streamData(publisher, options);
            publisher.submit(tuple(1L, "foo"));
        }

        assertThrows(ExecutionException.class, () -> streamFut.get(5, TimeUnit.SECONDS));
        logger.assertLogContains("Failed to refresh schemas and partition assignment");
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
}
