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

package org.apache.ignite.internal.table;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.storage.basic.ConcurrentHashMapStorage;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

/** */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TxTest extends IgniteAbstractTest {
    /** Table ID test value. */
    public final java.util.UUID tableId = java.util.UUID.randomUUID();

    /** */
    private static final NetworkAddress ADDR = new NetworkAddress("127.0.0.1", 2004);

    /** Accounts table. */
    private Table accounts;

    /** */
    public static final double BALANCE_1 = 500;

    /** */
    public static final double BALANCE_2 = 500;

    /** */
    public static final double DELTA = 100;

    /** */
    private IgniteTransactions igniteTransactions;

    @Mock
    private ClusterService clusterService;

    /** */
    private TxManager txManager;

    /** */
    private LockManager lockManager;

    /** */
    private InternalTable table;

    /**
     * Initialize the test state.
     */
    @BeforeEach
    public void before() {
        clusterService = Mockito.mock(ClusterService.class, RETURNS_DEEP_STUBS);
        Mockito.when(clusterService.topologyService().localMember().address()).thenReturn(ADDR);

        lockManager = new HeapLockManager();

        txManager = new TxManagerImpl(clusterService, lockManager);

        igniteTransactions = new IgniteTransactionsImpl(txManager);

        table = new DummyInternalTableImpl(new VersionedRowStore(new ConcurrentHashMapStorage(), txManager), txManager);

        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[]{new Column("accountNumber", NativeTypes.INT64, false)},
            new Column[]{new Column("balance", NativeTypes.DOUBLE, false)}
        );

        accounts = new TableImpl(table, new DummySchemaManagerImpl(schema), null, null);
    }

    /**
     * Tests a transaction closure.
     */
    @Test
    public void testTxClosure() throws TransactionException {
        accounts.upsert(makeValue(1, BALANCE_1));
        accounts.upsert(makeValue(2, BALANCE_2));

        igniteTransactions.runInTransaction(tx -> {
            Table txAcc = accounts.withTransaction(tx); // TODO asch Disallow implicit tx.

            CompletableFuture<Tuple> read1 = txAcc.getAsync(makeKey(1));
            CompletableFuture<Tuple> read2 = txAcc.getAsync(makeKey(2));

            txAcc.upsertAsync(makeValue(1, read1.join().doubleValue("balance") - DELTA));
            txAcc.upsertAsync(makeValue(2, read2.join().doubleValue("balance") + DELTA));
        });

        assertEquals(BALANCE_1 - DELTA, accounts.get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.get(makeKey(2)).doubleValue("balance"));
    }

    /**
     * Tests a transaction closure over key-value view.
     */
    @Test
    public void testTxClosureKeyValueView() throws TransactionException {
        accounts.upsert(makeValue(1, BALANCE_1));
        accounts.upsert(makeValue(2, BALANCE_2));

        igniteTransactions.runInTransaction(tx -> {
            KeyValueBinaryView txAcc = accounts.kvView().withTransaction(tx);

            CompletableFuture<Tuple> read1 = txAcc.getAsync(makeKey(1));
            CompletableFuture<Tuple> read2 = txAcc.getAsync(makeKey(2));

            txAcc.putAsync(makeKey(1), makeValue(1, read1.join().doubleValue("balance") - DELTA));
            txAcc.putAsync(makeKey(2), makeValue(2, read2.join().doubleValue("balance") + DELTA));
        });

        assertEquals(BALANCE_1 - DELTA, accounts.get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.get(makeKey(2)).doubleValue("balance"));
    }

    /**
     * Tests an asynchronous transaction.
     */
    @Test
    public void testTxAsync() {
        accounts.upsert(makeValue(1, BALANCE_1));
        accounts.upsert(makeValue(2, BALANCE_2));

        igniteTransactions.beginAsync().thenApply(tx -> accounts.withTransaction(tx)).
            thenCompose(txAcc -> txAcc.getAsync(makeKey(1))
                .thenCombine(txAcc.getAsync(makeKey(2)), (v1, v2) -> new Pair<>(v1, v2))
                .thenCompose(pair -> allOf(
                    txAcc.upsertAsync(makeValue(1, pair.getFirst().doubleValue("balance") - DELTA)),
                    txAcc.upsertAsync(makeValue(2, pair.getSecond().doubleValue("balance") + DELTA))
                    )
                )
                .thenApply(ignored -> txAcc.transaction())
            ).thenCompose(Transaction::commitAsync).join();

        assertEquals(BALANCE_1 - DELTA, accounts.get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.get(makeKey(2)).doubleValue("balance"));
    }

    /**
     * Tests an asynchronous transaction over key-value view.
     */
    @Test
    public void testTxAsyncKeyValueView() {
        accounts.upsert(makeValue(1, BALANCE_1));
        accounts.upsert(makeValue(2, BALANCE_2));

        igniteTransactions.beginAsync().thenApply(tx -> accounts.withTransaction(tx).kvView()).
            thenCompose(txAcc -> txAcc.getAsync(makeKey(1))
                .thenCombine(txAcc.getAsync(makeKey(2)), (v1, v2) -> new Pair<>(v1, v2))
                .thenCompose(pair -> allOf(
                    txAcc.putAsync(makeKey(1), makeValue(1, pair.getFirst().doubleValue("balance") - DELTA)),
                    txAcc.putAsync(makeKey(2), makeValue(2, pair.getSecond().doubleValue("balance") + DELTA))
                    )
                )
                .thenApply(ignored -> txAcc.transaction())
            ).thenCompose(Transaction::commitAsync).join();

        assertEquals(BALANCE_1 - DELTA, accounts.get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.get(makeKey(2)).doubleValue("balance"));
    }

    /** */
    @Test
    public void testSimpleConflict() throws Exception {
        accounts.upsert(makeValue(1, 100.));

        Transaction tx = igniteTransactions.beginAsync().get();
        Transaction tx2 = igniteTransactions.beginAsync().get();

        Table table = accounts.withTransaction(tx);
        Table table2 = accounts.withTransaction(tx2);

        double val = table.get(makeKey(1)).doubleValue("balance");
        double val2 = table2.get(makeKey(1)).doubleValue("balance");

        try {
            table.upsert(makeValue(1, val + 1));

            fail();
        }
        catch (Exception e) {
            // Expected.
        }

        table2.upsert(makeValue(1, val + 1));

        tx2.commit();

        try {
            tx.commit();

            fail();
        }
        catch (TransactionException e) {
            // Expected.
        }

        assertEquals(101., accounts.get(makeKey(1)).doubleValue("balance"));
    }

    /** */
    @Test
    public void testCommit() throws TransactionException {
        InternalTransaction tx = txManager.begin();

        Tuple key = makeKey(1);

        Table table = accounts.withTransaction(tx);

        table.upsert(makeValue(1, 100.));

        assertEquals(100., table.get(key).doubleValue("balance"));

        table.upsert(makeValue(1, 200.));

        assertEquals(200., table.get(key).doubleValue("balance"));

        tx.commit();

        assertEquals(200., accounts.get(key).doubleValue("balance"));

        assertEquals(COMMITED, txManager.state(tx.timestamp()));
    }

    /** */
    @Test
    public void testAbort() throws TransactionException {
        InternalTransaction tx = txManager.begin();

        Tuple key = makeKey(1);

        Table table = accounts.withTransaction(tx);

        table.upsert(makeValue(1, 100.));

        assertEquals(100., table.get(key).doubleValue("balance"));

        table.upsert(makeValue(1, 200.));

        assertEquals(200., table.get(key).doubleValue("balance"));

        tx.rollback();

        assertNull(accounts.get(key));

        assertEquals(ABORTED, txManager.state(tx.timestamp()));
    }

    /** */
    @Test
    public void testConcurrent() throws TransactionException {
        InternalTransaction tx = txManager.begin();
        InternalTransaction tx2 = txManager.begin();

        Tuple key = makeKey(1);
        Tuple val = makeValue(1, 100.);

        accounts.upsert(val);

        Table table = accounts.withTransaction(tx);
        Table table2 = accounts.withTransaction(tx2);

        assertEquals(100., table.get(key).doubleValue("balance"));
        assertEquals(100., table2.get(key).doubleValue("balance"));

        tx.commit();
        tx2.commit();
    }

    /**
     * Tests if a lost update is not happening on concurrent increment.
     *
     * @throws TransactionException
     */
    @Test
    public void testIncrement() throws TransactionException {
        InternalTransaction tx = txManager.begin();
        InternalTransaction tx2 = txManager.begin();

        Tuple key = makeKey(1);
        Tuple val = makeValue(1, 100.);

        accounts.upsert(val); // Creates implicit transaction.

        Table table = accounts.withTransaction(tx);
        Table table2 = accounts.withTransaction(tx2);

        // Read in tx
        double val_tx = table.get(key).doubleValue("balance");

        // Read in tx2
        double val_tx2 = table2.get(key).doubleValue("balance");

        // Write in tx (out of order)
        try {
            table.upsert(makeValue(1, val_tx + 1));

            fail();
        }
        catch (Exception e) { // TODO asch fix exception model.
            assertTrue(hasCause(e, LockException.class, null));
        }

        // Write in tx2
        table2.upsert(makeValue(1, val_tx2 + 1));

        tx2.commit();

        assertEquals(101., accounts.get(key).doubleValue("balance"));
    }

    /**
     * Tests if a lost update is not happening on concurrent increment.
     *
     * @throws TransactionException
     */
    @Test
    public void testIncrement2() throws TransactionException {
        InternalTransaction tx = txManager.begin();
        InternalTransaction tx2 = txManager.begin();

        Tuple key = makeKey(1);
        Tuple val = makeValue(1, 100.);

        accounts.upsert(val); // Creates implicit transaction.

        Table table = accounts.withTransaction(tx);
        Table table2 = accounts.withTransaction(tx2);

        // Read in tx
        double val_tx = table.get(key).doubleValue("balance");

        // Read in tx2
        double val_tx2 = table2.get(key).doubleValue("balance");

        // Write in tx2 (should wait for read unlock in tx1)
        CompletableFuture<Void> fut = table2.upsertAsync(makeValue(1, val_tx2 + 1));
        assertFalse(fut.isDone());

        CompletableFuture<Void> fut2 = fut.thenCompose(ret -> tx2.commitAsync());

        // Write in tx
        table.upsert(makeValue(1, val_tx + 1));

        tx.commit();

        try {
            fut2.join();
        }
        catch (Exception e) {
            assertTrue(hasCause(e, LockException.class, null));
        }

        assertEquals(101., accounts.get(key).doubleValue("balance"));
    }

    /** */
    @Test
    public void testAbortWithValue() throws TransactionException {
        accounts.upsert(makeValue(0, 100.));

        assertEquals(100., accounts.get(makeKey(0)).doubleValue("balance"));

        InternalTransaction tx = txManager.begin();
        Table table = accounts.withTransaction(tx);
        table.upsert(makeValue(0, 200.));
        assertEquals(200., table.get(makeKey(0)).doubleValue("balance"));
        tx.rollback();

        assertEquals(100., accounts.get(makeKey(0)).doubleValue("balance"));
    }

    /** */
    @Test
    public void testManyUpdates() throws TransactionException {
        InternalTransaction tx = txManager.begin();

        Table table = accounts.withTransaction(tx);

        table.upsert(makeValue(1, 100.));
        log.info("1 balance={}", table.get(makeKey(1)).doubleValue("balance"));

        table.upsert(makeValue(1, 200.));
        log.info("2 balance={}", table.get(makeKey(1)).doubleValue("balance"));

        table.upsert(makeValue(1, 300.));
        log.info("3 balance={}", table.get(makeKey(1)).doubleValue("balance"));
    }

    @Test
    public void testReorder() throws Exception {
        accounts.upsert(makeValue(1, 100.));

        InternalTransaction tx = txManager.begin();
        InternalTransaction tx2 = txManager.begin();
        InternalTransaction tx3 = txManager.begin();

        Table table = accounts.withTransaction(tx);
        Table table2 = accounts.withTransaction(tx2);
        Table table3 = accounts.withTransaction(tx3);

        double v0 = table.get(makeKey(1)).doubleValue("balance");
        double v1 = table3.get(makeKey(1)).doubleValue("balance");

        assertEquals(v0, v1);

        CompletableFuture<Void> fut = table3.upsertAsync(makeValue(1, v0 + 10));
        assertFalse(fut.isDone());

        table.upsert(makeValue(1, v0 + 20));

        CompletableFuture<Tuple> fut2 = table2.getAsync(makeKey(1));
        assertFalse(fut2.isDone());

        tx.commit();

        fut2.get();

        tx2.rollback();

        assertTrue(hasCause(assertThrows(ExecutionException.class, () -> fut.get()), LockException.class, null),
            "Wrong exception type, expecting LockException");
    }

    @Test
    public void testReorder2() throws Exception {
        accounts.upsert(makeValue(1, 100.));

        InternalTransaction tx = txManager.begin();
        InternalTransaction tx2 = txManager.begin();
        InternalTransaction tx3 = txManager.begin();

        Table table = accounts.withTransaction(tx);
        Table table2 = accounts.withTransaction(tx2);
        Table table3 = accounts.withTransaction(tx3);

        double v0 = table.get(makeKey(1)).doubleValue("balance");

        table.upsertAsync(makeValue(1, v0 + 10));

        CompletableFuture<Tuple> fut = table2.getAsync(makeKey(1));
        assertFalse(fut.isDone());

        CompletableFuture<Tuple> fut2 = table3.getAsync(makeKey(1));
        assertFalse(fut2.isDone());
    }

    /** */
    @Test
    public void testBalance() throws InterruptedException {
        doTestSingleKeyMultithreaded(5_000, false);
    }

    /**
     * @param duration The duration.
     * @param verbose Verbose mode.
     * @throws InterruptedException If interrupted while waiting.
     */
    private void doTestSingleKeyMultithreaded(long duration, boolean verbose) throws InterruptedException {
        int threadsCnt = Runtime.getRuntime().availableProcessors() * 2;

        Thread[] threads = new Thread[threadsCnt];

        final double initial = 1000;
        final double total = threads.length * initial;

        for (int i = 0; i < threads.length; i++)
            accounts.upsert(makeValue(i, 1000));

        double total0 = 0;

        for (long i = 0; i < threads.length; i++) {
            double balance = accounts.get(makeKey(i)).doubleValue("balance");

            total0 += balance;
        }

        assertEquals(total, total0, "Total amount invariant is not preserved");

        CyclicBarrier startBar = new CyclicBarrier(threads.length, () -> log.info("Before test"));

        CyclicBarrier sync = new CyclicBarrier(threads.length, new Runnable() {
            @Override public void run() {
                assertTrue(lockManager.isEmpty());

                if (verbose)
                    log.info("Sync");

                double total0 = 0;

                for (long i = 0; i < threads.length; i++) {
                    double balance = accounts.get(makeKey(i)).doubleValue("balance");

                    if (verbose)
                        log.info("Balance id={} val={}", i, balance);

                    total0 += balance;
                }

                if (total0 != total)
                    System.out.println();
            }
        });

        LongAdder ops = new LongAdder();
        LongAdder fails = new LongAdder();

        AtomicBoolean stop = new AtomicBoolean();

        Random r = new Random();

        AtomicReference<Throwable> firstErr = new AtomicReference<>();

        for (int i = 0; i < threads.length; i++) {
            long finalI = i;
            threads[i] = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        startBar.await();
                    }
                    catch (Exception e) {
                        fail();
                    }

                    while (!stop.get() && firstErr.get() == null) {
                        InternalTransaction tx = txManager.begin();

                        Table table = accounts.withTransaction(tx);

                        try {
                            long acc1 = finalI;

                            double amount = 100 + r.nextInt(500);

                            if (verbose)
                                log.info("op=tryGet ts={} id={}", tx.timestamp(), acc1);

                            double val0 = table.get(makeKey(acc1)).doubleValue("balance");

                            long acc2 = acc1;

                            while(acc1 == acc2)
                                acc2 = r.nextInt(threads.length);

                            if (verbose)
                                log.info("op=tryGet ts={} id={}", tx.timestamp(), acc2);

                            double val1 = table.get(makeKey(acc2)).doubleValue("balance");

                            if (verbose)
                                log.info("op=tryPut ts={} id={}", tx.timestamp(), acc1);

                            table.upsert(makeValue(acc1, val0 - amount));

                            if (verbose)
                                log.info("op=tryPut ts={} id={}", tx.timestamp(), acc2);

                            table.upsert(makeValue(acc2, val1 + amount));

                            tx.commit();

                            assertTrue(txManager.state(tx.timestamp()) == COMMITED);

                            ops.increment();
                        }
                        catch (Exception e) {
                            assertTrue(IgniteTestUtils.hasCause(e, LockException.class, null),
                                "Wrong exception type, expecting LockException");

                            fails.increment();
                        }
                    }
                }
            });

            threads[i].setName("Worker" + i);
            threads[i].setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override public void uncaughtException(Thread t, Throwable e) {
                    firstErr.compareAndExchange(null, e);
                }
            });
            threads[i].start();
        }

        Thread.sleep(duration);

        stop.set(true);

        for (Thread thread : threads)
            thread.join(3_000);

        if (firstErr.get() != null)
            throw new IgniteException(firstErr.get());

        log.info("After test ops={} fails={}", ops.sum(), fails.sum());

        total0 = 0;

        for (long i = 0; i < threads.length; i++) {
            double balance = accounts.get(makeKey(i)).doubleValue("balance");

            total0 += balance;
        }

        assertEquals(total, total0, "Total amount invariant is not preserved");
    }

    /**
     * @param id The id.
     * @return The key tuple.
     */
    private Tuple makeKey(long id) {
        return accounts.tupleBuilder().set("accountNumber", id).build();
    }

    /**
     * @param id The id.
     * @param balance The balance.
     * @return The value tuple.
     */
    private Tuple makeValue(long id, double balance) {
        return accounts.tupleBuilder().set("accountNumber", id).set("balance", balance).build();
    }
}
