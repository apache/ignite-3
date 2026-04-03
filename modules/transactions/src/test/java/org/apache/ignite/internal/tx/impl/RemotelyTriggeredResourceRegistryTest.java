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

package org.apache.ignite.internal.tx.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class RemotelyTriggeredResourceRegistryTest extends BaseIgniteAbstractTest {

    private final RemotelyTriggeredResourceRegistry registry = new RemotelyTriggeredResourceRegistry();

    @Test
    void registerAndCloseById() throws Exception {
        UUID contextId = UUID.randomUUID();
        UUID remoteHostId = UUID.randomUUID();
        FullyQualifiedResourceId id = new FullyQualifiedResourceId(contextId, UUID.randomUUID());

        AtomicInteger closedCount = new AtomicInteger();
        registry.register(id, remoteHostId, () -> () -> closedCount.incrementAndGet());

        assertEquals(1, registry.resources().size());

        registry.close(id);

        assertEquals(1, closedCount.get());
        assertEquals(0, registry.resources().size());
    }

    @Test
    void registerReturnsSameResourceOnDuplicateCall() throws Exception {
        UUID contextId = UUID.randomUUID();
        UUID remoteHostId = UUID.randomUUID();
        FullyQualifiedResourceId id = new FullyQualifiedResourceId(contextId, UUID.randomUUID());

        AtomicInteger creationCount = new AtomicInteger();
        registry.register(id, remoteHostId, () -> {
            creationCount.incrementAndGet();
            return () -> {};
        });
        registry.register(id, remoteHostId, () -> {
            creationCount.incrementAndGet();
            return () -> {};
        });

        assertEquals(1, creationCount.get());
        assertEquals(1, registry.resources().size());
    }

    @Test
    void closeByContextIdClosesAllResourcesInContext() throws Exception {
        UUID contextId = UUID.randomUUID();
        UUID remoteHostId = UUID.randomUUID();

        AtomicInteger closedCount = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            FullyQualifiedResourceId id = new FullyQualifiedResourceId(contextId, UUID.randomUUID());
            registry.register(id, remoteHostId, () -> () -> closedCount.incrementAndGet());
        }

        assertEquals(5, registry.resources().size());
        registry.close(contextId);

        assertEquals(5, closedCount.get());
        assertEquals(0, registry.resources().size());
    }

    @Test
    void closeByRemoteHostIdClosesAllResourcesFromThatHost() throws Exception {
        UUID remoteHostId = UUID.randomUUID();
        AtomicInteger closedCount = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            FullyQualifiedResourceId id = new FullyQualifiedResourceId(UUID.randomUUID(), UUID.randomUUID());
            registry.register(id, remoteHostId, () -> () -> closedCount.incrementAndGet());
        }

        assertEquals(3, registry.resources().size());
        registry.closeByRemoteHostId(remoteHostId);

        assertEquals(3, closedCount.get());
        assertEquals(0, registry.resources().size());
    }

    @Test
    void closeByRemoteHostIdIsNoOpForUnknownHost() throws Exception {
        assertDoesNotThrow(() -> registry.closeByRemoteHostId(UUID.randomUUID()));
        assertEquals(0, registry.resources().size());
    }

    @Test
    void closeByContextIdAfterCloseByRemoteHostIdIsNoOp() throws Exception {
        UUID contextId = UUID.randomUUID();
        UUID remoteHostId = UUID.randomUUID();
        AtomicInteger closedCount = new AtomicInteger();

        FullyQualifiedResourceId id = new FullyQualifiedResourceId(contextId, UUID.randomUUID());
        registry.register(id, remoteHostId, () -> () -> closedCount.incrementAndGet());

        registry.closeByRemoteHostId(remoteHostId);
        registry.close(contextId); // must be a no-op

        assertEquals(1, closedCount.get());
        assertEquals(0, registry.resources().size());
    }

    @Test
    void closeByIdIsNoOpForUnknownResource() throws Exception {
        FullyQualifiedResourceId id = new FullyQualifiedResourceId(UUID.randomUUID(), UUID.randomUUID());
        assertDoesNotThrow(() -> registry.close(id));
        assertEquals(0, registry.resources().size());
    }

    @Test
    void closeByContextIdIsNoOpForUnknownContext() throws Exception {
        assertDoesNotThrow(() -> registry.close(UUID.randomUUID()));
        assertEquals(0, registry.resources().size());
    }

    @Test
    void multipleContextsSameRemoteHost() throws Exception {
        UUID remoteHostId = UUID.randomUUID();
        UUID contextId1 = UUID.randomUUID();
        UUID contextId2 = UUID.randomUUID();

        AtomicInteger closedCount = new AtomicInteger();
        registry.register(new FullyQualifiedResourceId(contextId1, UUID.randomUUID()), remoteHostId,
                () -> () -> closedCount.incrementAndGet());
        registry.register(new FullyQualifiedResourceId(contextId2, UUID.randomUUID()), remoteHostId,
                () -> () -> closedCount.incrementAndGet());

        registry.close(contextId1); // closes one context, host entry must survive

        assertEquals(1, closedCount.get());
        assertEquals(1, registry.resources().size());
        assertEquals(1, registry.registeredRemoteHosts().size()); // host still has contextId2

        registry.close(contextId2);

        assertEquals(2, closedCount.get());
        assertEquals(0, registry.resources().size());
        assertEquals(0, registry.registeredRemoteHosts().size()); // host entry cleaned up
    }

    @Test
    void concurrentRegistrationWithSameContextAndHost() throws Exception {
        UUID contextId = UUID.randomUUID();
        UUID remoteHostId = UUID.randomUUID();
        int threadCount = 32;

        List<FullyQualifiedResourceId> ids = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            ids.add(new FullyQualifiedResourceId(contextId, UUID.randomUUID()));
        }

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (FullyQualifiedResourceId id : ids) {
                futures.add(executor.submit(() -> {
                    try {
                        start.await();
                        registry.register(id, remoteHostId, () -> () -> {});
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals(threadCount, registry.resources().size());
        assertEquals(1, registry.registeredRemoteHosts().size());
    }
}
