/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.causality;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteTriConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests of causality token implementation based on versioned value.
 * {@link VersionedValue}
 */
public class VersionedValueTest {
    /** Test value. */
    public static final int TEST_VALUE = 1;

    /** The test revision register is used to move the revision forward. */
    public static final TestRevisionRegister REGISTER = new TestRevisionRegister();

    @BeforeEach
    public void clearRegister() {
        REGISTER.clear();
    }

    /**
     * The test gets a value for {@link VersionedValue} before the value is calculated.
     *
     * @throws OutdatedTokenException If failed.
     */
    @Test
    public void testGetValueBeforeReady() throws OutdatedTokenException {
        VersionedValue<Integer> intVersionedValue = new VersionedValue<>(REGISTER, 2, null);

        CompletableFuture<Integer> fut = intVersionedValue.get(0);

        assertFalse(fut.isDone());

        intVersionedValue.complete(0L, TEST_VALUE);

        REGISTER.moveRevision(0L).join();

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.join());

        assertSame(fut.join(), intVersionedValue.get(0).join());
    }

    /**
     * The test explicitly sets a value to {@link VersionedValue} without waiting for the revision update.
     *
     * @throws OutdatedTokenException If failed.
     */
    @Test
    public void testExplicitlySetValue() throws OutdatedTokenException {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>(REGISTER);

        CompletableFuture<Integer> fut = longVersionedValue.get(0);

        assertFalse(fut.isDone());

        longVersionedValue.complete(0, TEST_VALUE);

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.join());

        assertSame(fut.join(), longVersionedValue.get(0).join());
    }

    /**
     * The test reads a value with the specific token in which the value should not be updated.
     * The read happenes before the revision updated.
     *
     * @throws OutdatedTokenException If failed.
     */
    @Test
    public void testMissValueUpdateBeforeReady() throws OutdatedTokenException {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>(REGISTER);

        longVersionedValue.complete(0, TEST_VALUE);

        REGISTER.moveRevision(0L).join();

        CompletableFuture<Integer> fut = longVersionedValue.get(1);

        assertFalse(fut.isDone());

        REGISTER.moveRevision(1L).join();

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.join());

        assertSame(fut.join(), longVersionedValue.get(0).join());
    }

    /**
     * The test reads a value with the specific token in which the value should not be updated.
     * The read happens after the revision updated.
     *
     * @throws OutdatedTokenException If failed.
     */
    @Test
    public void testMissValueUpdate() throws OutdatedTokenException {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>(REGISTER);

        longVersionedValue.complete(0, TEST_VALUE);

        REGISTER.moveRevision(0L).join();
        REGISTER.moveRevision(1L).join();

        CompletableFuture<Integer> fut = longVersionedValue.get(1);

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.join());

        assertSame(fut, longVersionedValue.get(0));
    }

    /**
     * Test checks token history size.
     */
    @Test
    public void testObsoleteToken() {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>(REGISTER);

        longVersionedValue.complete(0, TEST_VALUE);

        REGISTER.moveRevision(0L).join();

        longVersionedValue.complete(1, TEST_VALUE);

        REGISTER.moveRevision(1L).join();
        REGISTER.moveRevision(2L).join();

        assertThrowsExactly(OutdatedTokenException.class, () -> longVersionedValue.get(0));
    }

    /**
     * Checks that future will be completed automatically when the related token becomes actual.
     */
    @Test
    public void testAutocompleteFuture() throws OutdatedTokenException {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>(REGISTER);

        longVersionedValue.complete(0, TEST_VALUE);

        REGISTER.moveRevision(0L).join();

        CompletableFuture<Integer> fut = longVersionedValue.get(1);

        assertFalse(fut.isDone());

        REGISTER.moveRevision(1L).join();
        REGISTER.moveRevision(2L).join();

        assertTrue(fut.isDone());
        assertTrue(longVersionedValue.get(2).isDone());
    }

    /**
     * Checks that the update method work as expected when the previous value is known.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate() throws Exception {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>(REGISTER);

        longVersionedValue.complete(0, TEST_VALUE);

        REGISTER.moveRevision(0L).join();

        CompletableFuture<Integer> fut = longVersionedValue.get(1);

        assertFalse(fut.isDone());

        int incrementCount = 10;

        for (int i = 0; i < incrementCount; i++) {
            longVersionedValue.update(1, (previous, e) -> completedFuture(++previous));

            assertFalse(fut.isDone());
        }

        REGISTER.moveRevision(1L).join();

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE + incrementCount, fut.get());

        assertThrows(AssertionError.class, () -> longVersionedValue.update(1L, (i, t) -> completedFuture(null)));
    }

    /**
     * Checks that the update method work as expected when there is no history to calculate previous value.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdatePredefined() throws Exception {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>(REGISTER);

        CompletableFuture<Integer> fut = longVersionedValue.get(0);

        assertFalse(fut.isDone());

        longVersionedValue.update(0, (previous, e) -> {
            assertNull(previous);

            return completedFuture(TEST_VALUE);
        });

        assertFalse(fut.isDone());

        REGISTER.moveRevision(0L).join();

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.get());
    }

    /**
     * Test asynchronous update closure.
     */
    @Test
    public void testAsyncUpdate() {
        VersionedValue<Integer> vv = new VersionedValue<>(REGISTER);

        CompletableFuture<Integer> fut = new CompletableFuture<>();

        vv.update(0L, (v, e) -> fut);

        CompletableFuture<Integer> vvFut = vv.get(0L);

        CompletableFuture<?> revFut = REGISTER.moveRevision(0L);

        assertFalse(fut.isDone());
        assertFalse(vvFut.isDone());
        assertFalse(revFut.isDone());

        fut.complete(1);

        revFut.join();

        assertTrue(vvFut.isDone());
    }

    /**
     * Test the case when exception happens in updater.
     */
    @Test
    public void testExceptionOnUpdate() {
        VersionedValue<Integer> vv = new VersionedValue<>(REGISTER, () -> 0);

        final int count = 4;
        final int successfulCompletionsCount = count / 2;

        AtomicInteger actualSuccessfulCompletionsCount = new AtomicInteger();

        final String exceptionMsg = "test msg";

        for (int i = 0; i < count; i++) {
            vv.update(0L, (v, e) -> {
                if (e != null) {
                    return failedFuture(e);
                }

                if (v == successfulCompletionsCount) {
                    throw new IgniteInternalException(exceptionMsg);
                }

                actualSuccessfulCompletionsCount.incrementAndGet();

                return completedFuture(++v);
            });
        }

        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();

        vv.whenComplete((t, v, e) -> exceptionRef.set(e));

        vv.complete(0L);

        assertThrowsWithCause(() -> vv.get(0L).join(), IgniteInternalException.class);

        assertEquals(exceptionMsg, exceptionRef.get().getMessage());
        assertEquals(successfulCompletionsCount, actualSuccessfulCompletionsCount.get());
    }

    /**
     * Test with multiple versioned values and asynchronous completion.
     */
    @Test
    public void testAsyncMultiVv() {
        final String registryName = "Registry";
        final String assignmentName = "Assignment";
        final String tableName = "T1_";

        VersionedValue<Map<UUID, String>> tablesVv = new VersionedValue<>(f -> {}, HashMap::new);
        VersionedValue<Map<UUID, String>> schemasVv = new VersionedValue<>(REGISTER, HashMap::new);
        VersionedValue<Map<UUID, String>> assignmentsVv = new VersionedValue<>(REGISTER, HashMap::new);

        schemasVv.whenComplete((token, value, ex) -> tablesVv.complete(token));

        BiFunction<Long, UUID, CompletableFuture<String>> schemaRegistry =
                (token, uuid) -> schemasVv.get(token).thenApply(schemas -> schemas.get(uuid));

        // Adding table.
        long token = 0L;
        UUID tableId = UUID.randomUUID();

        CompletableFuture<String> tableFut = schemaRegistry.apply(token, tableId)
                .thenCombine(assignmentsVv.get(token), (registry, assignments) -> tableName + registry + assignments.get(tableId));

        tablesVv.update(token, (old, e) -> tableFut.thenApply(table -> {
            Map<UUID, String> val = new HashMap<>(old);

            val.put(tableId, table);

            return val;
        }));

        CompletableFuture<String> userFut = tablesVv.get(token).thenApply(map -> map.get(tableId));

        schemasVv.update(token, (old, e) -> {
            old.put(tableId, registryName);

            return completedFuture(old);
        });

        assignmentsVv.update(token, (old, e) -> {
            old.put(tableId, assignmentName);

            return completedFuture(old);
        });

        assertFalse(tableFut.isDone());
        assertFalse(userFut.isDone());

        REGISTER.moveRevision(token).join();

        tableFut.join();

        assertEquals(tableName + registryName + assignmentName, userFut.join());
    }

    /**
     * Checks a behavior when {@link VersionedValue} has not initialized yet, but someone already tries to get a value.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInitialization() throws Exception {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>(REGISTER);

        CompletableFuture<Integer> fut1 = longVersionedValue.get(1);
        CompletableFuture<Integer> fut2 = longVersionedValue.get(2);

        assertFalse(fut1.isDone());
        assertFalse(fut2.isDone());

        assertNull(longVersionedValue.latest());

        longVersionedValue.complete(2, TEST_VALUE);

        assertTrue(fut1.isDone());
        assertTrue(fut2.isDone());

        assertThrowsExactly(ExecutionException.class, fut1::get);
        assertEquals(TEST_VALUE, fut2.get());
    }

    /**
     * Tests a default value supplier.
     */
    @Test
    public void testDefaultValueSupplier() {
        VersionedValue<Integer> vv = new VersionedValue<>(REGISTER, () -> TEST_VALUE);

        checkDefaultValue(vv, TEST_VALUE);
    }

    /**
     * Tests a case when there is no default value supplier.
     */
    @Test
    public void testWithoutDefaultValue() {
        VersionedValue<Integer> vv = new VersionedValue<>(REGISTER);

        checkDefaultValue(vv, null);
    }

    /**
     * Tests a case when there is no default value supplier.
     */
    public void checkDefaultValue(VersionedValue<Integer> vv, Integer expectedDefault) {
        assertEquals(expectedDefault, vv.latest());

        vv.update(0, (a, e) -> {
                    assertEquals(expectedDefault, vv.latest());

                    return completedFuture(a == null ? null : a + 1);
                }
        );

        assertEquals(expectedDefault, vv.latest());

        CompletableFuture<Integer> f = vv.get(0);

        assertFalse(f.isDone());

        vv.update(0, (a, e) -> completedFuture(a == null ? null : a + 1));

        REGISTER.moveRevision(0L).join();

        assertTrue(f.isDone());

        assertEquals(expectedDefault == null ? null : TEST_VALUE + 2, f.join());
    }

    /**
     * Test {@link VersionedValue#whenComplete(IgniteTriConsumer)}.
     */
    @Test
    public void testWhenComplete() {
        VersionedValue<Integer> vv = new VersionedValue<>(REGISTER);

        AtomicInteger a = new AtomicInteger();
        AtomicInteger cntr = new AtomicInteger(-1);

        IgniteTriConsumer<Long, Integer, Throwable> listener = (t, v, e) -> {
            if (e == null) {
                a.set(v);
            } else {
                a.set(-1);
            }

            cntr.incrementAndGet();
        };

        vv.whenComplete(listener);

        // Test complete.
        long token = 0;

        final long finalToken0 = token;

        vv.complete(token, TEST_VALUE);

        assertThrows(AssertionError.class, () -> vv.complete(finalToken0, 0));
        assertThrows(AssertionError.class, () -> vv.completeExceptionally(finalToken0, new Exception()));

        assertEquals(TEST_VALUE, a.get());
        assertEquals(token, cntr.get());

        REGISTER.moveRevision(token).join();

        // Test update.
        token = 1;

        vv.update(token, (v, e) -> completedFuture(++v));

        assertEquals(TEST_VALUE, a.get());

        REGISTER.moveRevision(token).join();

        assertEquals(TEST_VALUE + 1, a.get());
        assertEquals(token, cntr.get());

        // Test move revision.
        token = 2;

        REGISTER.moveRevision(token).join();

        assertEquals(TEST_VALUE + 1, a.get());
        assertEquals(token, cntr.get());

        // Test complete exceptionally.
        token = 3;

        final long finalToken3 = token;

        vv.completeExceptionally(token, new Exception());

        assertThrows(AssertionError.class, () -> vv.complete(finalToken3, 0));
        assertThrows(AssertionError.class, () -> vv.completeExceptionally(finalToken3, new Exception()));

        assertEquals(-1, a.get());
        assertEquals(token, cntr.get());

        REGISTER.moveRevision(token).join();

        assertEquals(token, cntr.get());

        // Test remove listener.
        token = 4;

        vv.removeWhenComplete(listener);

        a.set(0);

        vv.complete(token, TEST_VALUE);

        assertEquals(0, a.get());
        assertEquals(token - 1, cntr.get());

        REGISTER.moveRevision(token).join();
    }

    /**
     * Test revision register.
     */
    private static class TestRevisionRegister implements Consumer<Function<Long, CompletableFuture<?>>> {
        /** Revision consumer. */
        List<Function<Long, CompletableFuture<?>>> moveRevisionList = new ArrayList<>();

        /** {@inheritDoc} */
        @Override
        public void accept(Function<Long, CompletableFuture<?>> function) {
            moveRevisionList.add(function);
        }

        /**
         * Clear list.
         */
        public void clear() {
            moveRevisionList.clear();
        }

        /**
         * Move revision.
         *
         * @param revision Revision.
         * @return Future for all listeners.
         */
        public CompletableFuture<?> moveRevision(long revision) {
            List<CompletableFuture<?>> futures = new ArrayList<>();

            moveRevisionList.forEach(m -> futures.add(m.apply(revision)));

            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}));
        }
    }
}
