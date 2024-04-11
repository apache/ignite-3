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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link InternalTable#scan(int, org.apache.ignite.internal.tx.InternalTransaction)}.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
public abstract class ItAbstractInternalTableScanTest extends IgniteAbstractTest {
    private static final SchemaDescriptor ROW_SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.stringOf(100), false)},
            new Column[]{new Column("val", NativeTypes.stringOf(100), false)}
    );

    @InjectConfiguration
    private TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private StorageUpdateConfiguration storageUpdateConfiguration;

    /** Mock partition storage. */
    @Mock
    private MvPartitionStorage mockStorage;

    /** Internal table to test. */
    DummyInternalTableImpl internalTbl;

    /**
     * Prepare test environment using DummyInternalTableImpl and Mocked storage.
     */
    @BeforeEach
    public void setUp(TestInfo testInfo) {
        internalTbl = new DummyInternalTableImpl(mock(ReplicaService.class), mockStorage, ROW_SCHEMA, txConfiguration,
                storageUpdateConfiguration);
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
    @Test
    public void testNegativeRequestedAmountScan() throws Exception {
        invalidRequestNtest(-1);
    }

    /**
     * Checks whether {@link IllegalArgumentException} is thrown and inner storage cursor is closes in case of zero requested amount of
     * items.
     *
     * @throws Exception If any.
     */
    @Test
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

        when(mockStorage.scan(any(HybridTimestamp.class))).thenAnswer(invocation -> {
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

        InternalTransaction tx = startTx();

        scan(0, tx).subscribe(new Subscriber<>() {

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

        assertTrue(subscriberFinishedLatch.await(10, TimeUnit.SECONDS), "count=" + subscriberFinishedLatch.getCount());

        assertEquals(gotException.get().getCause().getClass(), NoSuchElementException.class);

        validateTxAbortedState(tx);
    }

    /**
     * Checks that exception from storage cursor creation properly propagates to subscriber.
     */
    @Test
    public void testExceptionRowScan() throws Exception {
        // The latch that allows to await Subscriber.onError() before asserting test invariants.
        CountDownLatch gotExceptionLatch = new CountDownLatch(1);

        AtomicReference<Throwable> gotException = new AtomicReference<>();

        when(mockStorage.scan(any(HybridTimestamp.class))).thenThrow(new StorageException("Some storage exception"));

        InternalTransaction tx = startTx();

        scan(0, tx).subscribe(new Subscriber<>() {

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

        assertTrue(gotExceptionLatch.await(10_000, TimeUnit.MILLISECONDS));

        assertEquals(gotException.get().getCause().getClass(), StorageException.class);

        validateTxAbortedState(tx);
    }

    protected void validateTxAbortedState(InternalTransaction tx) {
        if (tx != null) {
            assertEquals(TxState.ABORTED, tx.state());
        }
    }

    /**
     * Checks that in case of second subscription {@link IllegalStateException} will be fires to onError.
     *
     * @throws Exception If any.
     */
    @Test
    public void testSecondSubscriptionFiresIllegalStateException() throws Exception {
        Flow.Publisher<BinaryRow> scan = scan(0, startTx());

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

        assertTrue(gotExceptionLatch.await(10_000, TimeUnit.MILLISECONDS));

        assertEquals(gotException.get().getClass(), IllegalStateException.class);
    }

    /**
     * Checks that {@link NullPointerException} is thrown in case of null subscription.
     */
    @Test
    public void testNullPointerExceptionIsThrownInCaseOfNullSubscription() {
        assertThrows(
                NullPointerException.class,
                () -> scan(0, null).subscribe(null)
        );
    }

    /**
     * Helper method to convert key and value to {@link BinaryRow}.
     *
     * @param entryKey Key.
     * @param entryVal Value
     * @return {@link BinaryRow} based on given key and value.
     */
    private static BinaryRow prepareRow(String entryKey, String entryVal) {
        return new RowAssembler(ROW_SCHEMA, -1)
                .appendString(Objects.requireNonNull(entryKey, "entryKey"))
                .appendString(Objects.requireNonNull(entryVal, "entryVal"))
                .build();
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

        when(mockStorage.scan(any(HybridTimestamp.class))).thenAnswer(invocation -> {
            var cursor = mock(PartitionTimestampCursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> cursorTouchCnt.get() < submittedItems.size());

            lenient().when(cursor.next()).thenAnswer(ninvocation ->
                    ReadResult.createFromCommitted(new RowId(0), submittedItems.get(cursorTouchCnt.getAndIncrement()),
                            internalTbl.CLOCK.now()));

            return cursor;
        });

        // The latch that allows to await Subscriber.onError() before asserting test invariants.
        CountDownLatch subscriberAllDataAwaitLatch = new CountDownLatch(1);

        InternalTransaction tx = startTx();

        scan(0, tx).subscribe(new Subscriber<>() {
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

        assertThat(submittedItems, is(retrievedItems));

        if (tx != null) {
            tx.commit();
        }
    }

    /**
     * Start transaction if needed.
     *
     * @return Started transaction or {@code null}.
     */
    protected InternalTransaction startTx() {
        return null;
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

        lenient().when(mockStorage.scan(any(HybridTimestamp.class))).thenAnswer(invocation -> {
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

        scan(0, startTx()).subscribe(new Subscriber<>() {
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

    /**
     * Either read-write or read-only publisher producer.
     *
     * @param part  The partition.
     * @param tx The transaction.
     * @return {@link Publisher} that reactively notifies about partition rows.
     */
    protected abstract Publisher<BinaryRow> scan(int part, @Nullable InternalTransaction tx);
}
