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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClient.Builder;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

/**
 * Tests thin client configuration.
 */
public class ConfigurationTest extends AbstractClientTest {
    @Test
    public void testClientValidatesAddresses() {
        IgniteClient.Builder builder = IgniteClient.builder();

        var ex = assertThrows(IgniteException.class, builder::build);

        assertThat(ex.getMessage(), containsString("Empty addresses"));
    }

    @Test
    public void testClientValidatesPorts() {
        IgniteClient.Builder builder = IgniteClient.builder()
                .addresses("127.0.0.1:70000");

        var ex = assertThrows(IgniteException.class, builder::build);

        assertThat(
                ex.getMessage(),
                containsString("Failed to parse Ignite server address (invalid port 70000): 127.0.0.1:70000"));
    }

    @Test
    public void testAddressFinderWorksWithoutAddresses() {
        String addr = "127.0.0.1:" + serverPort;

        IgniteClient.Builder builder = IgniteClient.builder();

        IgniteClient client = builder
                .addressFinder(() -> new String[]{addr})
                .build();

        try (client) {
            assertArrayEquals(new String[]{addr}, client.configuration().addressesFinder().getAddresses());
        }
    }

    @Test
    public void testClientBuilderPropagatesAllConfigurationValues() {
        String addr = "127.0.0.1:" + serverPort;

        IgniteClient.Builder builder = IgniteClient.builder();

        IgniteClient client = builder
                .addresses(addr)
                .connectTimeout(1234)
                .addressFinder(() -> new String[]{addr})
                .name("thin-client")
                .build();

        // Builder can be reused, and it won't affect already created clients.
        IgniteClient client2 = builder
                .connectTimeout(2345)
                .build();

        try (client) {
            // Check that client works.
            assertEquals(0, client.tables().tables().size());

            // Check config values.
            assertEquals("thin-client", client.name());
            assertEquals(1234, client.configuration().connectTimeout());
            assertArrayEquals(new String[]{addr}, client.configuration().addresses());
            assertArrayEquals(new String[]{addr}, client.configuration().addressesFinder().getAddresses());
        }

        try (client2) {
            // Check that client works.
            assertEquals(0, client2.tables().tables().size());

            // Check config values.
            assertEquals(2345, client2.configuration().connectTimeout());
            assertArrayEquals(new String[]{addr}, client.configuration().addresses());
            assertArrayEquals(new String[]{addr}, client.configuration().addressesFinder().getAddresses());
        }
    }

    @Test
    public void testClientBuilderFailsOnExceptionInAddressFinder() {
        IgniteClient.Builder builder = IgniteClient.builder()
                .addressFinder(() -> {
                    throw new IllegalArgumentException("bad finder");
                });

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testDefaultAsyncContinuationExecutorIsForkJoinPool() {
        String threadName = client.tables().tablesAsync().thenApply(unused -> Thread.currentThread().getName()).join();

        assertNull(client.configuration().asyncContinuationExecutor());

        // Current thread is used when future completes quickly.
        assertThat(threadName, either(startsWith("ForkJoinPool.commonPool-worker-")).or(equalTo(Thread.currentThread().getName())));
    }

    @Test
    public void testDirectAsyncContinuationExecutorUsesNettyThread() {
        IgniteClient.Builder builder = IgniteClient.builder()
                .addresses("127.0.0.1:" + serverPort)
                .asyncContinuationExecutor(Runnable::run);

        try (IgniteClient ignite = builder.build()) {
            String threadName = ignite.tables().tablesAsync().thenApply(unused -> Thread.currentThread().getName()).join();

            // Current thread is used when future completes quickly.
            assertThat(threadName, either(startsWith("nioEventLoopGroup-")).or(equalTo(Thread.currentThread().getName())));
        }
    }

    @Test
    public void testCustomAsyncContinuationExecutor() {
        Function<Integer, Integer> responseDelay = x -> 50;

        try (var testServer = new TestServer(0, server, x -> false, responseDelay, "n2", clusterId, null, null)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();

            var builderThreadName = new AtomicReference<String>();

            CompletableFuture<IgniteClient> builder = IgniteClient.builder()
                    .addresses("127.0.0.1:" + testServer.port())
                    .asyncContinuationExecutor(executor)
                    .buildAsync()
                    .whenComplete((res, err) -> builderThreadName.set(Thread.currentThread().getName()));

            try (IgniteClient ignite = builder.join()) {
                String threadName = ignite.tables().tablesAsync().thenApply(unused -> Thread.currentThread().getName()).join();

                assertEquals(executor, ignite.configuration().asyncContinuationExecutor());
                assertThat(threadName, startsWith("pool-"));
                assertThat(builderThreadName.get(), startsWith("pool-"));
            }

            executor.shutdown();
        }
    }

    @Test
    public void testAsyncContinuationExecutorException() {
        AtomicLong reqId = new AtomicLong();

        Builder clientBuilder = IgniteClient.builder()
                .addresses("localhost:" + serverPort)
                .asyncContinuationExecutor(command -> {
                    if (reqId.incrementAndGet() > 2) {
                        throw new RuntimeException("bad executor");
                    }

                    command.run();
                });

        try (var client = clientBuilder.build()) {
            IgniteException ex = assertThrows(IgniteException.class, () -> client.tables().tables());
            assertThat(ex.getMessage(), containsString("bad executor"));
        }
    }
}
