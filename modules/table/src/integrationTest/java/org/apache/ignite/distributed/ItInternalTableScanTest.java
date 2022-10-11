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

package org.apache.ignite.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.util.ByteUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link InternalTable#scan(int, org.apache.ignite.internal.tx.InternalTransaction)}.
 */
@ExtendWith(MockitoExtension.class)
public class ItInternalTableScanTest {
    /** Mock partition storage. */
    @Mock
    private MvPartitionStorage mockStorage;

    /** Internal table to test. */
    private InternalTable internalTbl;

    /**
     * Prepare test environment using DummyInternalTableImpl and Mocked storage.
     */
    @BeforeEach
    public void setUp(TestInfo testInfo) {
        internalTbl = new DummyInternalTableImpl(Mockito.mock(ReplicaService.class), mockStorage);
    }

    /**
     * Checks whether publisher provides all existing data and then completes if requested by one row at a time.
     */
    @Test
    public void testOneRowScan() throws Exception {
        requestNtest(
                List.of(
                        prepareRow("key1", "val1"),
                        prepareRow("key2", "val2")
                ),
                1);
    }

    /**
     * Checks whether publisher provides all existing data and then completes if requested by multiple rows at a time.
     */
    @Test
    public void testMultipleRowScan() throws Exception {
        requestNtest(
                List.of(
                        prepareRow("key1", "val1"),
                        prepareRow("key2", "val2"),
                        prepareRow("key3", "val3"),
                        prepareRow("key4", "val4"),
                        prepareRow("key5", "val5")
                ),
                2);
    }

    /**
     * Checks whether publisher provides all existing data and then completes if requested by Long.MAX_VALUE rows at a time.
     */
    @Test
    public void testLongMaxValueRowScan() throws Exception {
        requestNtest(
                List.of(
                        prepareRow("key1", "val1"),
                        prepareRow("key2", "val2"),
                        prepareRow("key3", "val3"),
                        prepareRow("key4", "val4"),
                        prepareRow("key5", "val5")
                ),
                Long.MAX_VALUE);
    }

    /**
     * Checks whether {@link IllegalArgumentException} is thrown and inner storage cursor is closes in case of negative requested amount of
     * items.
     *
     * @throws Exception If any.
     */
    @Test()
    public void testNegativeRequestedAmountScan() throws Exception {
        invalidRequestNtest(-1);
    }

    /**
     * Checks whether {@link IllegalArgumentException} is thrown and inner storage cursor is closes in case of zero requested amount of
     * items.
     *
     * @throws Exception If any.
     */
    @Test()
    public void testZeroRequestedAmountScan() throws Exception {
        invalidRequestNtest(0);
    }

    /**
     * Checks that exception from storage cursors has next properly propagates to subscriber.
     */
    @Test
    public void testExceptionRowScanCursorHasNext() throws Exception {
        // The latch that allows to await Subscriber.onComplete() before asserting test invariants
        // and avoids the race between closing the cursor and stopping the node.
        CountDownLatch subscriberFinishedLatch = new CountDownLatch(2);

        AtomicReference<Throwable> gotException = new AtomicReference<>();

        when(mockStorage.scan(any(), any(HybridTimestamp.class))).thenAnswer(invocation -> {
            var cursor = mock(PartitionTimestampCursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> true);

            when(cursor.next()).thenAnswer(hnInvocation -> {
                throw new NoSuchElementException("test");
            });

            doAnswer(
                    invocationClose -> {
                        subscriberFinishedLatch.countDown();
                        return null;
                    }
            ).when(cursor).close();

            return cursor;
        });

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {

            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(BinaryRow item) {
                fail("Should never get here.");
            }

            @Override
            public void onError(Throwable throwable) {
                gotException.set(throwable);
                subscriberFinishedLatch.countDown();
            }

            @Override
            public void onComplete() {
                fail("Should never get here.");
            }
        });

        subscriberFinishedLatch.await();

        assertEquals(gotException.get().getCause().getClass(), NoSuchElementException.class);
    }

    /**
     * Checks that exception from storage cursor creation properly propagates to subscriber.
     */
    @Test
    public void testExceptionRowScan() throws Exception {
        // The latch that allows to await Subscriber.onError() before asserting test invariants.
        CountDownLatch gotExceptionLatch = new CountDownLatch(1);

        AtomicReference<Throwable> gotException = new AtomicReference<>();

        when(mockStorage.scan(any(), any(HybridTimestamp.class))).thenThrow(new StorageException("Some storage exception"));

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {

            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(BinaryRow item) {
                fail("Should never get here.");
            }

            @Override
            public void onError(Throwable throwable) {
                gotException.set(throwable);
                gotExceptionLatch.countDown();
            }

            @Override
            public void onComplete() {
                fail("Should never get here.");
            }
        });

        gotExceptionLatch.await();

        assertEquals(gotException.get().getCause().getClass(), StorageException.class);
    }


    /**
     * Checks that {@link IllegalArgumentException} is thrown in case of invalid partition.
     */
    @Test()
    public void testInvalidPartitionParameterScan() {
        assertThrows(
                IllegalArgumentException.class,
                () -> internalTbl.scan(-1, null)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> internalTbl.scan(1, null)
        );
    }

    /**
     * Checks that in case of second subscription {@link IllegalStateException} will be fires to onError.
     *
     * @throws Exception If any.
     */
    @Test
    public void testSecondSubscriptionFiresIllegalStateException() throws Exception {
        Flow.Publisher<BinaryRow> scan = internalTbl.scan(0, null);

        scan.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {

            }

            @Override
            public void onNext(BinaryRow item) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });

        // The latch that allows to await Subscriber.onError() before asserting test invariants.
        CountDownLatch gotExceptionLatch = new CountDownLatch(1);

        AtomicReference<Throwable> gotException = new AtomicReference<>();

        scan.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {

            }

            @Override
            public void onNext(BinaryRow item) {

            }

            @Override
            public void onError(Throwable throwable) {
                gotException.set(throwable);
                gotExceptionLatch.countDown();
            }

            @Override
            public void onComplete() {

            }
        });

        gotExceptionLatch.await();

        assertEquals(gotException.get().getClass(), IllegalStateException.class);
    }

    /**
     * Checks that {@link NullPointerException} is thrown in case of null subscription.
     */
    @Test
    public void testNullPointerExceptionIsThrownInCaseOfNullSubscription() {
        assertThrows(
                NullPointerException.class,
                () -> internalTbl.scan(0, null).subscribe(null)
        );
    }

    /**
     * Helper method to convert key and value to {@link DataRow}.
     *
     * @param entryKey Key.
     * @param entryVal Value
     * @return {@link DataRow} based on given key and value.
     * @throws java.io.IOException If failed to close output stream that was used to convertation.
     */
    private static @NotNull BinaryRow prepareRow(@NotNull String entryKey,
            @NotNull String entryVal) throws IOException {
        byte[] keyBytes = ByteUtils.toBytes(entryKey);

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            outputStream.write(keyBytes);
            outputStream.write(ByteUtils.toBytes(entryVal));

            return new ByteBufferRow(keyBytes);
        }
    }

    /**
     * Checks whether publisher provides all existing data and then completes if requested by reqAmount rows at a time.
     *
     * @param submittedItems Items to be pushed by publisher.
     * @param reqAmount      Amount of rows to request at a time.
     * @throws Exception If Any.
     */
    private void requestNtest(List<BinaryRow> submittedItems, long reqAmount) throws Exception {
        AtomicInteger cursorTouchCnt = new AtomicInteger(0);

        List<BinaryRow> retrievedItems = Collections.synchronizedList(new ArrayList<>());

        when(mockStorage.scan(any(), any(HybridTimestamp.class))).thenAnswer(invocation -> {
            var cursor = mock(PartitionTimestampCursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> cursorTouchCnt.get() < submittedItems.size());

            when(cursor.next()).thenAnswer(ninvocation ->
                    ReadResult.createFromCommitted(submittedItems.get(cursorTouchCnt.getAndIncrement())));

            return cursor;
        });

        // The latch that allows to await Subscriber.onError() before asserting test invariants.
        CountDownLatch subscriberAllDataAwaitLatch = new CountDownLatch(1);

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;

                subscription.request(reqAmount);
            }

            @Override
            public void onNext(BinaryRow item) {
                retrievedItems.add(item);

                if (retrievedItems.size() % reqAmount == 0) {
                    subscription.request(reqAmount);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                fail("onError call is not expected.");
            }

            @Override
            public void onComplete() {
                subscriberAllDataAwaitLatch.countDown();
            }
        });

        subscriberAllDataAwaitLatch.await();

        assertEquals(submittedItems.size(), retrievedItems.size());

        List<byte[]> expItems = submittedItems.stream().map(BinaryRow::bytes).collect(Collectors.toList());
        List<byte[]> gotItems = retrievedItems.stream().map(BinaryRow::bytes).collect(Collectors.toList());

        for (int i = 0; i < expItems.size(); i++) {
            assertTrue(Arrays.equals(expItems.get(i), gotItems.get(i)));
        }
    }

    /**
     * Checks whether {@link IllegalArgumentException} is thrown and inner storage cursor is closes in case of invalid requested amount of
     * items.
     *
     * @param reqAmount  Amount of rows to request at a time.
     * @throws Exception If Any.
     */
    private void invalidRequestNtest(int reqAmount) throws InterruptedException {
        // The latch that allows to await Subscriber.onComplete() before asserting test invariants
        // and avoids the race between closing the cursor and stopping the node.
        CountDownLatch subscriberFinishedLatch = new CountDownLatch(1);

        lenient().when(mockStorage.scan(any(), any(HybridTimestamp.class))).thenAnswer(invocation -> {
            var cursor = mock(PartitionTimestampCursor.class);

            doAnswer(
                    invocationClose -> {
                        subscriberFinishedLatch.countDown();
                        return null;
                    }
            ).when(cursor).close();

            when(cursor.hasNext()).thenAnswer(hnInvocation -> {
                throw new StorageException("test");
            });

            return cursor;
        });

        AtomicReference<Throwable> gotException = new AtomicReference<>();

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(reqAmount);
            }

            @Override
            public void onNext(BinaryRow item) {
                fail("Should never get here.");
            }

            @Override
            public void onError(Throwable throwable) {
                gotException.set(throwable);
                subscriberFinishedLatch.countDown();
            }

            @Override
            public void onComplete() {
                fail("Should never get here.");
            }
        });

        subscriberFinishedLatch.await();

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    throw gotException.get();
                }
        );
    }
}
