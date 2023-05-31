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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
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
    public void testBasicStreaming(int batchSize) {
        RecordView<Tuple> view = defaultTable().recordView();

        var publisher = new SubmissionPublisher<Tuple>();
        var options = DataStreamerOptions.builder().batchSize(batchSize).build();
        CompletableFuture<Void> fut = view.streamData(publisher, options);

        publisher.submit(tuple(1L, "foo"));
        publisher.submit(tuple(2L, "bar"));

        publisher.close();
        fut.orTimeout(1, TimeUnit.SECONDS).join();

        assertNotNull(view.get(null, tupleKey(1L)));
        assertNotNull(view.get(null, tupleKey(2L)));
        assertNull(view.get(null, tupleKey(3L)));

        assertEquals("bar", view.get(null, tupleKey(2L)).stringValue("name"));
    }

    @Test
    public void testBasicStreamingPojo() {
        RecordView<PersonPojo> view = defaultTable().recordView(PersonPojo.class);
        CompletableFuture<Void> fut;

        try (var publisher = new SubmissionPublisher<PersonPojo>()) {
            fut = view.streamData(publisher, null);

            publisher.submit(new PersonPojo(1L, "foo"));
            publisher.submit(new PersonPojo(2L, "bar"));
        }

        fut.orTimeout(1, TimeUnit.SECONDS).join();
        assertEquals("bar", view.get(null, new PersonPojo(2L)).name);
    }

    @Test
    public void testBasicStreamingKv() {
        assert false;
    }

    @Test
    public void testBasicStreamingKvPojo() {
        assert false;
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
    public void testBackPressure() throws Exception {
        var server2 = new FakeIgnite("server-2");

        Function<Integer, Integer> responseDelay = idx -> idx > 5 ? 500 : 0;
        testServer2 = new TestServer(10900, 10, 10_000, server2, idx -> false, responseDelay, null, UUID.randomUUID(), null);

        var port = testServer2.port();

        client2 = IgniteClient.builder().addresses("localhost:" + port).build();
        RecordView<Tuple> view = defaultTableView(server2, client2);

        var bufferSize = 2;
        var publisher = new SubmissionPublisher<Tuple>(ForkJoinPool.commonPool(), bufferSize);

        var options = DataStreamerOptions.builder()
                .batchSize(bufferSize)
                .perNodeParallelOperations(1)
                .build();

        var streamFut = view.streamData(publisher, options);

        // Stream 10 items while buffer capacity is 2 to trigger back pressure.
        var submitFut = CompletableFuture.runAsync(() -> {
            for (long i = 0; i < 10; i++) {
                publisher.submit(tuple(i, "foo_" + i));
            }
        });

        // Due to `responseDelay` above, `publisher.submit` is blocking when buffer is full => submitFut can't complete in 200 ms.
        assertThrows(TimeoutException.class, () -> submitFut.get(200, TimeUnit.MILLISECONDS));
        assertFalse(streamFut.isDone());
    }

    @Test
    public void testPartitionAwareness() {
        // TODO: See how PartitionAwarenessTest is implemented using setDataAccessListener
        assert false;
    }

    @Test
    public void testManyItemsWithDisconnectAndRetry() throws Exception {
        var server2 = new FakeIgnite("server-2");

        // Drop connection on every 5th request.
        Function<Integer, Boolean> shouldDropConnection = idx -> idx % 5 == 4;
        Function<Integer, Integer> responseDelay = idx -> 0;
        testServer2 = new TestServer(10900, 10, 10_000, server2, shouldDropConnection, responseDelay, null, UUID.randomUUID(), null);

        // Streamer has it's own retry policy, so we can disable retries on the client.
        Builder builder = IgniteClient.builder()
                .addresses("localhost:" + testServer2.port())
                .retryPolicy(new RetryLimitPolicy().retryLimit(0))
                .reconnectThrottlingPeriod(0)
                .loggerFactory(new ConsoleLoggerFactory("client-2"));

        client2 = builder.build();
        RecordView<Tuple> view = defaultTableView(server2, client2);
        CompletableFuture<Void> streamFut;

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            var options = DataStreamerOptions.builder().batchSize(2).build();
            streamFut = view.streamData(publisher, options);

            for (long i = 0; i < 1000; i++) {
                publisher.submit(tuple(i, "foo_" + i));
            }
        }

        streamFut.get(5000, TimeUnit.SECONDS);

        for (long i = 0; i < 100; i++) {
            assertNotNull(view.get(null, tupleKey(i)), "Failed to get tuple: " + i);
        }
    }

    @Test
    public void testAssignmentRefreshErrorClosesStreamer() {
        // TODO:
        assert false;
    }

    private static RecordView<Tuple> defaultTableView(FakeIgnite server, IgniteClient client) {
        ((FakeIgniteTables) server.tables()).createTable(DEFAULT_TABLE);

        return client.tables().table(DEFAULT_TABLE).recordView();
    }
}
