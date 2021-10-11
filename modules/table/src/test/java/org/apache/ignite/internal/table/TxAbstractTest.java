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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public abstract class TxAbstractTest extends IgniteAbstractTest {
    /** */
    protected static SchemaDescriptor ACCOUNTS_SCHEMA = new SchemaDescriptor(
        1,
        new Column[]{new Column("accountNumber", NativeTypes.INT64, false)},
        new Column[]{new Column("balance", NativeTypes.DOUBLE, false)}
    );

    /** Table ID test value. */
    public static final UUID tableId2 = java.util.UUID.randomUUID();

    /** */
    protected static SchemaDescriptor CUSTOMERS_SCHEMA = new SchemaDescriptor(
        1,
        new Column[]{new Column("accountNumber", NativeTypes.INT64, false)},
        new Column[]{new Column("name", NativeTypes.STRING, false)}
    );

    /** */
    protected static final NetworkAddress ADDR = new NetworkAddress("127.0.0.1", 2004);

    /** Accounts table id -> balance. */
    protected Table accounts;

    /** Customers table id -> name. */
    protected Table customers;

    /** */
    protected static final double BALANCE_1 = 500;

    /** */
    protected static final double BALANCE_2 = 500;

    /** */
    protected static final double DELTA = 100;

    /** */
    protected IgniteTransactions igniteTransactions;

    /**
     * Initialize the test state.
     */
    @BeforeEach
    public abstract void before() throws Exception;

    /** */
    @Test
    public void testMixedPutGet() throws TransactionException {
        accounts.recordView().upsert(makeValue(1, BALANCE_1));

        igniteTransactions.runInTransaction(tx -> {
            var txAcc = accounts.recordView().withTransaction(tx);

            txAcc.getAsync(makeKey(1)).thenCompose(r ->
                txAcc.upsertAsync(makeValue(1, r.doubleValue("balance") + DELTA))).join();
        });

        assertEquals(BALANCE_1 + DELTA, accounts.recordView().get(makeKey(1)).doubleValue("balance"));
    }

    /**
     * Tests a transaction closure.
     */
    @Test
    public void testTxClosure() throws TransactionException {
        RecordView<Tuple> view = accounts.recordView();

        view.upsert(makeValue(1, BALANCE_1));
        view.upsert(makeValue(2, BALANCE_2));

        igniteTransactions.runInTransaction(tx -> {
            CompletableFuture<Tuple> read1 = view.getAsync(makeKey(1));
            CompletableFuture<Tuple> read2 = view.getAsync(makeKey(2));

            view.upsertAsync(makeValue(1, read1.join().doubleValue("balance") - DELTA));
            view.upsertAsync(makeValue(2, read2.join().doubleValue("balance") + DELTA));
        });

        assertEquals(BALANCE_1 - DELTA, view.get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, view.get(makeKey(2)).doubleValue("balance"));

        assertEquals(5, txManager(accounts).finished());
    }

    /**
     * Tests a transaction closure over key-value view.
     */
    @Test
    public void testTxClosureKeyValueView() throws TransactionException {
        accounts.recordView().upsert(makeValue(1, BALANCE_1));
        accounts.recordView().upsert(makeValue(2, BALANCE_2));

        igniteTransactions.runInTransaction(tx -> {
            KeyValueView<Tuple, Tuple> view = accounts.keyValueView();

            CompletableFuture<Tuple> read1 = view.getAsync(makeKey(1));
            CompletableFuture<Tuple> read2 = view.getAsync(makeKey(2));

            view.putAsync(makeKey(1), makeValue(read1.join().doubleValue("balance") - DELTA)).join();
            view.putAsync(makeKey(2), makeValue(read2.join().doubleValue("balance") + DELTA)).join();
        });

        assertEquals(BALANCE_1 - DELTA, accounts.recordView().get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.recordView().get(makeKey(2)).doubleValue("balance"));

        assertEquals(5, txManager(accounts).finished());
    }

    /**
     * Tests an asynchronous transaction.
     */
    @Test
    public void testTxAsync() {
        accounts.recordView().upsert(makeValue(1, BALANCE_1));
        accounts.recordView().upsert(makeValue(2, BALANCE_2));

        igniteTransactions.beginAsync().thenApply(tx -> accounts.recordView().withTransaction(tx)).
            thenCompose(txAcc -> txAcc.getAsync(makeKey(1))
                .thenCombine(txAcc.getAsync(makeKey(2)), (v1, v2) -> new Pair<>(v1, v2))
                .thenCompose(pair -> allOf(
                    txAcc.upsertAsync(makeValue(1, pair.getFirst().doubleValue("balance") - DELTA)),
                    txAcc.upsertAsync(makeValue(2, pair.getSecond().doubleValue("balance") + DELTA))
                    )
                )
                .thenApply(ignored -> txAcc.transaction())
            ).thenCompose(Transaction::commitAsync).join();

        assertEquals(BALANCE_1 - DELTA, accounts.recordView().get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.recordView().get(makeKey(2)).doubleValue("balance"));
    }

    /**
     * Tests an asynchronous transaction over key-value view.
     */
    @Test
    public void testTxAsyncKeyValueView() {
        accounts.recordView().upsert(makeValue(1, BALANCE_1));
        accounts.recordView().upsert(makeValue(2, BALANCE_2));

        igniteTransactions.beginAsync().thenApply(tx -> accounts.keyValueView().withTransaction(tx)).
            thenCompose(txAcc -> txAcc.getAsync(makeKey(1))
                .thenCombine(txAcc.getAsync(makeKey(2)), (v1, v2) -> new Pair<>(v1, v2))
                .thenCompose(pair -> allOf(
                    txAcc.putAsync(makeKey(1), makeValue(pair.getFirst().doubleValue("balance") - DELTA)),
                    txAcc.putAsync(makeKey(2), makeValue(pair.getSecond().doubleValue("balance") + DELTA))
                    )
                )
                .thenApply(ignored -> txAcc.transaction())
            ).thenCompose(Transaction::commitAsync).join();

        assertEquals(BALANCE_1 - DELTA, accounts.recordView().get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.recordView().get(makeKey(2)).doubleValue("balance"));
    }

    /** */
    @Test
    public void testSimpleConflict() throws Exception {
        accounts.recordView().upsert(makeValue(1, 100.));

        Transaction tx = igniteTransactions.begin();
        Transaction tx2 = igniteTransactions.begin();

        var table = accounts.recordView().withTransaction(tx);
        var table2 = accounts.recordView().withTransaction(tx2);

        double val = table.get(makeKey(1)).doubleValue("balance");
        table2.get(makeKey(1)).doubleValue("balance");

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

        assertEquals(101., accounts.recordView().get(makeKey(1)).doubleValue("balance"));
    }

    /** */
    @Test
    public void testCommit() throws TransactionException {
        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        Tuple key = makeKey(1);

        var table = accounts.recordView().withTransaction(tx);

        table.upsert(makeValue(1, 100.));

        assertEquals(100., table.get(key).doubleValue("balance"));

        table.upsert(makeValue(1, 200.));

        assertEquals(200., table.get(key).doubleValue("balance"));

        tx.commit();

        assertEquals(200., accounts.recordView().get(key).doubleValue("balance"));

        assertEquals(COMMITED, txManager(accounts).state(tx.timestamp()));
    }

    /** */
    @Test
    public void testAbort() throws TransactionException {
        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        Tuple key = makeKey(1);

        var table = accounts.recordView().withTransaction(tx);

        table.upsert(makeValue(1, 100.));

        assertEquals(100., table.get(key).doubleValue("balance"));

        table.upsert(makeValue(1, 200.));

        assertEquals(200., table.get(key).doubleValue("balance"));

        tx.rollback();

        assertNull(accounts.recordView().get(key));

        assertEquals(ABORTED, txManager(accounts).state(tx.timestamp()));
    }

    /** */
    @Test
    public void testAbortNoUpdate() throws TransactionException {
        accounts.recordView().upsert(makeValue(1, 100.));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        tx.rollback();

        assertEquals(100., accounts.recordView().get(makeKey(1)).doubleValue("balance"));
    }

    /** */
    @Test
    public void testConcurrent() throws TransactionException {
        Transaction tx = igniteTransactions.begin();
        Transaction tx2 = igniteTransactions.begin();

        Tuple key = makeKey(1);
        Tuple val = makeValue(1, 100.);

        accounts.recordView().upsert(val);

        var table = accounts.recordView().withTransaction(tx);
        var table2 = accounts.recordView().withTransaction(tx2);

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
        Transaction tx = igniteTransactions.begin();
        Transaction tx2 = igniteTransactions.begin();

        Tuple key = makeKey(1);
        Tuple val = makeValue(1, 100.);

        accounts.recordView().upsert(val); // Creates implicit transaction.

        var table = accounts.recordView().withTransaction(tx);
        var table2 = accounts.recordView().withTransaction(tx2);

        // Read in tx
        double val_tx = table.get(key).doubleValue("balance");

        // Read in tx2
        double val_tx2 = table2.get(key).doubleValue("balance");

        // Write in tx (out of order)
        // TODO asch fix exception model.
        Exception err = assertThrows(Exception.class, () -> table.upsert(makeValue(1, val_tx + 1)));

        assertTrue(err.getMessage().contains("LockException"), err.getMessage());

        // Write in tx2
        table2.upsert(makeValue(1, val_tx2 + 1));

        tx2.commit();

        assertEquals(101., accounts.recordView().get(key).doubleValue("balance"));
    }

    /**
     * Tests if a lost update is not happening on concurrent increment.
     *
     * @throws TransactionException
     */
    @Test
    public void testIncrement2() throws TransactionException {
        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();

        Tuple key = makeKey(1);
        Tuple val = makeValue(1, 100.);

        accounts.recordView().upsert(val); // Creates implicit transaction.

        var table = accounts.recordView().withTransaction(tx);
        var table2 = accounts.recordView().withTransaction(tx2);

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

        Exception err = assertThrows(Exception.class, () -> fut2.get(5, TimeUnit.SECONDS));

        assertTrue(err.getMessage().contains("LockException"), err.getMessage());

        assertEquals(101., accounts.recordView().get(key).doubleValue("balance"));
    }

    /** */
    @Test
    public void testAbortWithValue() throws TransactionException {
        accounts.recordView().upsert(makeValue(0, 100.));

        assertEquals(100., accounts.recordView().get(makeKey(0)).doubleValue("balance"));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        var table = accounts.recordView().withTransaction(tx);
        table.upsert(makeValue(0, 200.));
        assertEquals(200., table.get(makeKey(0)).doubleValue("balance"));
        tx.rollback();

        assertEquals(100., accounts.recordView().get(makeKey(0)).doubleValue("balance"));
    }

    /** */
    @Test
    public void testInsert() throws TransactionException {
        assertNull(accounts.recordView().get(makeKey(1)));

        Transaction tx = igniteTransactions.begin();

        var table = accounts.recordView().withTransaction(tx);

        assertTrue(table.insert(makeValue(1, 100.)));
        assertFalse(table.insert(makeValue(1, 200.)));
        assertEquals(100., table.get(makeKey(1)).doubleValue("balance"));

        tx.commit();

        assertEquals(100., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        assertTrue(accounts.recordView().insert(makeValue(2, 200.)));
        assertEquals(200., accounts.recordView().get(makeKey(2)).doubleValue("balance"));

        Transaction tx2 = igniteTransactions.begin();

        table = accounts.recordView().withTransaction(tx2);

        assertTrue(table.insert(makeValue(3, 100.)));
        assertFalse(table.insert(makeValue(3, 200.)));
        assertEquals(100., table.get(makeKey(3)).doubleValue("balance"));

        tx2.rollback();

        assertEquals(100., accounts.recordView().get(makeKey(1)).doubleValue("balance"));
        assertEquals(200., accounts.recordView().get(makeKey(2)).doubleValue("balance"));
        assertNull(accounts.recordView().get(makeKey(3)));
    }

    @Test
    public void testDelete() throws TransactionException {
        Tuple key = makeKey(1);

        assertFalse(accounts.recordView().delete(key));
        assertNull(accounts.recordView().get(key));

        accounts.recordView().upsert(makeValue(1, 100.));

        igniteTransactions.runInTransaction(tx -> {
            assertNotNull(accounts.recordView().get(key));
            assertTrue(accounts.recordView().delete(key));
            assertNull(accounts.recordView().get(key));
        });

        assertNull(accounts.recordView().get(key));
        accounts.recordView().upsert(makeValue(1, 100.));
        assertNotNull(accounts.recordView().get(key));

        Tuple key2 = makeKey(2);

        accounts.recordView().upsert(makeValue(2, 100.));

        assertThrows(RuntimeException.class, () -> igniteTransactions.runInTransaction((Consumer<Transaction>) tx -> {
            assertNotNull(accounts.recordView().get(key2));
            assertTrue(accounts.recordView().delete(key2));
            assertNull(accounts.recordView().get(key2));
            throw new RuntimeException(); // Triggers rollback.
        }));

        assertNotNull(accounts.recordView().get(key2));
        assertTrue(accounts.recordView().delete(key2));
        assertNull(accounts.recordView().get(key2));
    }

    @Test
    public void testReorder() throws Exception {
        accounts.recordView().upsert(makeValue(1, 100.));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx3 = (InternalTransaction) igniteTransactions.begin();

        var table = accounts.recordView().withTransaction(tx);
        var table2 = accounts.recordView().withTransaction(tx2);
        var table3 = accounts.recordView().withTransaction(tx3);

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

        Exception err = assertThrows(Exception.class, () -> fut.get(5, TimeUnit.SECONDS));

        assertTrue(err.getMessage().contains("LockException"), err.getMessage());
    }

    @Test
    public void testReorder2() throws Exception {
        accounts.recordView().upsert(makeValue(1, 100.));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx3 = (InternalTransaction) igniteTransactions.begin();

        var table = accounts.recordView().withTransaction(tx);
        var table2 = accounts.recordView().withTransaction(tx2);
        var table3 = accounts.recordView().withTransaction(tx3);

        double v0 = table.get(makeKey(1)).doubleValue("balance");

        table.upsertAsync(makeValue(1, v0 + 10));

        CompletableFuture<Tuple> fut = table2.getAsync(makeKey(1));
        assertFalse(fut.isDone());

        CompletableFuture<Tuple> fut2 = table3.getAsync(makeKey(1));
        assertFalse(fut2.isDone());
    }

    /** */
    @Test
    public void testCrossTable() throws TransactionException {
        customers.recordView().upsert(makeValue(1, "test"));
        accounts.recordView().upsert(makeValue(1, 100.));

        assertEquals("test", customers.recordView().get(makeKey(1)).stringValue("name"));
        assertEquals(100., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();

        var txCust = customers.recordView().withTransaction(tx);
        var txAcc = accounts.recordView().withTransaction(tx);

        txCust.upsert(makeValue(1, "test2"));
        txAcc.upsert(makeValue(1, 200.));

        Tuple txValCust = txCust.get(makeKey(1));
        assertEquals("test2", txValCust.stringValue("name"));

        txValCust.set("accountNumber", 2L);
        txValCust.set("name", "test3");

        Tuple txValAcc = txAcc.get(makeKey(1));
        assertEquals(200., txValAcc.doubleValue("balance"));

        txValAcc.set("accountNumber", 2L);
        txValAcc.set("balance", 300.);

        tx.commit();

        assertEquals("test2", customers.recordView().get(makeKey(1)).stringValue("name"));
        assertEquals(200., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        assertTrue(lockManager(accounts).isEmpty());
        assertTrue(lockManager(customers).isEmpty());
    }

    /** */
    @Test
    public void testTwoTables() throws TransactionException {
        customers.recordView().upsert(makeValue(1, "test"));
        accounts.recordView().upsert(makeValue(1, 100.));

        assertEquals("test", customers.recordView().get(makeKey(1)).stringValue("name"));
        assertEquals(100., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();

        var txCust = customers.recordView().withTransaction(tx);
        var txAcc = accounts.recordView().withTransaction(tx);

        txCust.upsert(makeValue(1, "test2"));
        txAcc.upsert(makeValue(1, 200.));

        Tuple txValCust = txCust.get(makeKey(1));
        assertEquals("test2", txValCust.stringValue("name"));

        txValCust.set("accountNumber", 2L);
        txValCust.set("name", "test3");

        Tuple txValAcc = txAcc.get(makeKey(1));
        assertEquals(200., txValAcc.doubleValue("balance"));

        txValAcc.set("accountNumber", 2L);
        txValAcc.set("balance", 300.);

        tx.commit();
        tx2.commit();

        assertEquals("test2", customers.recordView().get(makeKey(1)).stringValue("name"));
        assertEquals(200., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        assertTrue(lockManager(accounts).isEmpty());
    }

    /** */
    @Test
    public void testCrossTableKeyValueView() throws TransactionException {
        customers.recordView().upsert(makeValue(1L, "test"));
        accounts.recordView().upsert(makeValue(1L, 100.));

        assertEquals("test", customers.recordView().get(makeKey(1)).stringValue("name"));
        assertEquals(100., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        InternalTransaction tx = (InternalTransaction) igniteTransactions.begin();
        InternalTransaction tx2 = (InternalTransaction) igniteTransactions.begin();

        var txCust = customers.keyValueView().withTransaction(tx);
        var txAcc = accounts.keyValueView().withTransaction(tx);

        txCust.put(makeKey(1), makeValue("test2"));
        txAcc.put(makeKey(1), makeValue(200.));

        Tuple txValCust = txCust.get(makeKey(1));
        assertEquals("test2", txValCust.stringValue("name"));

        txValCust.set("accountNumber", 2L);
        txValCust.set("name", "test3");

        Tuple txValAcc = txAcc.get(makeKey(1));
        assertEquals(200., txValAcc.doubleValue("balance"));

        txValAcc.set("accountNumber", 2L);
        txValAcc.set("balance", 300.);

        tx.commit();
        tx2.commit();

        assertEquals("test2", customers.recordView().get(makeKey(1)).stringValue("name"));
        assertEquals(200., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        assertTrue(lockManager(accounts).isEmpty());
    }

    /** */
    @Test
    public void testCrossTableAsync() throws TransactionException {
        customers.recordView().upsert(makeValue(1, "test"));
        accounts.recordView().upsert(makeValue(1, 100.));

        igniteTransactions.beginAsync()
            .thenCompose(
                    tx -> accounts.recordView().withTransaction(tx).upsertAsync(makeValue(1, 200.))
                        .thenCombine(customers.recordView().withTransaction(tx).upsertAsync(makeValue(1, "test2")), (v1, v2) -> tx)
                )
                .thenCompose(Transaction::commitAsync).join();

        assertEquals("test2", customers.recordView().get(makeKey(1)).stringValue("name"));
        assertEquals(200., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        assertTrue(lockManager(accounts).isEmpty());
    }

    /** */
    @Test
    public void testCrossTableAsyncRollback() throws TransactionException {
        customers.recordView().upsert(makeValue(1, "test"));
        accounts.recordView().upsert(makeValue(1, 100.));

        igniteTransactions.beginAsync()
            .thenCompose(
                tx -> accounts.recordView().withTransaction(tx).upsertAsync(makeValue(1, 200.))
                    .thenCombine(customers.recordView().withTransaction(tx).upsertAsync(makeValue(1, "test2")), (v1, v2) -> tx)
            )
            .thenCompose(Transaction::rollbackAsync).join();

        assertEquals("test", customers.recordView().get(makeKey(1)).stringValue("name"));
        assertEquals(100., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        assertTrue(lockManager(accounts).isEmpty());
    }

    /** */
    @Test
    public void testCrossTableAsyncKeyValueView() throws TransactionException {
        customers.recordView().upsert(makeValue(1, "test"));
        accounts.recordView().upsert(makeValue(1, 100.));

        igniteTransactions.beginAsync()
            .thenCompose(
                tx -> accounts.keyValueView().withTransaction(tx).putAsync(makeKey(1), makeValue(200.))
                    .thenCombine(customers.keyValueView().withTransaction(tx).putAsync(makeKey(1), makeValue("test2")), (v1, v2) -> tx)
            )
            .thenCompose(Transaction::commitAsync).join();

        assertEquals("test2", customers.recordView().get(makeKey(1)).stringValue("name"));
        assertEquals(200., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        assertTrue(lockManager(accounts).isEmpty());
    }

    /** */
    @Test
    public void testCrossTableAsyncKeyValueViewRollback() throws TransactionException {
        customers.recordView().upsert(makeValue(1, "test"));
        accounts.recordView().upsert(makeValue(1, 100.));

        igniteTransactions.beginAsync()
            .thenCompose(
                tx -> accounts.keyValueView().withTransaction(tx).putAsync(makeKey(1), makeValue(200.))
                    .thenCombine(customers.keyValueView().withTransaction(tx).putAsync(makeKey(1), makeValue("test2")), (v1, v2) -> tx)
            )
            .thenCompose(Transaction::rollbackAsync).join();

        assertEquals("test", customers.recordView().get(makeKey(1)).stringValue("name"));
        assertEquals(100., accounts.recordView().get(makeKey(1)).doubleValue("balance"));

        assertTrue(lockManager(accounts).isEmpty());
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
            accounts.recordView().upsert(makeValue(i, 1000));

        double total0 = 0;

        for (long i = 0; i < threads.length; i++) {
            double balance = accounts.recordView().get(makeKey(i)).doubleValue("balance");

            total0 += balance;
        }

        assertEquals(total, total0, "Total amount invariant is not preserved");

        CyclicBarrier startBar = new CyclicBarrier(threads.length, () -> log.info("Before test"));

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
                        InternalTransaction tx = txManager(accounts).begin();

                        var table = accounts.recordView().withTransaction(tx);

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

                            assertTrue(txManager(accounts).state(tx.timestamp()) == COMMITED);

                            ops.increment();
                        }
                        catch (Exception e) {
                            assertTrue(e.getMessage().contains("LockException"), e.getMessage());

                            fails.increment();
                        }
                    }
                }
            });

            threads[i].setName("Worker-" + i);
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
            double balance = accounts.recordView().get(makeKey(i)).doubleValue("balance");

            total0 += balance;
        }

        assertEquals(total, total0, "Total amount invariant is not preserved");
    }

    /**
     * @param id The id.
     * @return The key tuple.
     */
    private Tuple makeKey(long id) {
        return Tuple.create().set("accountNumber", id);
    }

    /**
     * @param id The id.
     * @param balance The balance.
     * @return The value tuple.
     */
    private Tuple makeValue(long id, double balance) {
        return Tuple.create().set("accountNumber", id).set("balance", balance);
    }

    /**
     * @param id The id.
     * @param balance The balance.
     * @return The value tuple.
     */
    private Tuple makeValue(long id, String name) {
        return Tuple.create().set("accountNumber", id).set("name", name);
    }

    /**
     * @param id The id.
     * @param balance The balance.
     * @return The value tuple.
     */
    private Tuple makeValue(double balance) {
        return Tuple.create().set("balance", balance);
    }

    /**
     * @param id The id.
     * @param name The name.
     * @return The value tuple.
     */
    private Tuple makeValue(String name) {
        return Tuple.create().set("name", name);
    }

    /**
     * Get a tx manager for test purposes.
     *
     * @param t The table.
     * @return TX manager.
     */
    protected abstract TxManager txManager(Table t);

    /**
     * Get a lock manager for test purposes.
     *
     * @param t The table.
     * @return Lock manager.
     */
    protected LockManager lockManager(Table t) {
        return ((TxManagerImpl)txManager(t)).getLockManager();
    }


    /**
     * @param t The table.
     * @param partId Partition id.
     * @return {@code True} if a replicas are the same.
     */
    protected abstract boolean assertPartitionsSame(Table t, int partId);
}
