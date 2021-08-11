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

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.storage.basic.ConcurrentHashMapStorage;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

/** */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TxTest {
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
    @Mock
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

        txManager = new TxManagerImpl(clusterService);

        lockManager = new HeapLockManager();

        table = new DummyInternalTableImpl(new VersionedRowStore(new ConcurrentHashMapStorage(), txManager, lockManager));

        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[]{new Column("accountNumber", NativeTypes.INT64, false)},
            new Column[]{new Column("balance", NativeTypes.DOUBLE, false)}
        );

        accounts = new TableImpl(table, new DummySchemaManagerImpl(schema), null, null);
        Tuple r1 = accounts.tupleBuilder().set("accountNumber", 1L).set("balance", BALANCE_1).build();
        Tuple r2 = accounts.tupleBuilder().set("accountNumber", 2L).set("balance", BALANCE_2).build();

        accounts.insert(r1);
        accounts.insert(r2);

        Transaction tx = txManager.begin();

        Mockito.doAnswer(invocation -> {
            Consumer<Transaction> argument = invocation.getArgument(0);

            argument.accept(tx);

            tx.commit();

            return null;
        }).when(igniteTransactions).runInTransaction(Mockito.any());

        Mockito.when(igniteTransactions.beginAsync()).thenReturn(CompletableFuture.completedFuture(tx));
    }

    /**
     * Tests a synchronous transaction.
     */
    @Test
    public void testTxSync() {
        igniteTransactions.runInTransaction(tx -> {
            Table txAcc = accounts.withTransaction(tx);

            CompletableFuture<Tuple> read1 = txAcc.getAsync(makeKey(1));
            CompletableFuture<Tuple> read2 = txAcc.getAsync(makeKey(2));

            txAcc.upsertAsync(makeValue(1, read1.join().doubleValue("balance") - DELTA));
            txAcc.upsertAsync(makeValue(2, read2.join().doubleValue("balance") + DELTA));
        });

        assertEquals(BALANCE_1 - DELTA, accounts.get(makeKey(1)).doubleValue("balance"));
        assertEquals(BALANCE_2 + DELTA, accounts.get(makeKey(2)).doubleValue("balance"));
    }

    /**
     * Tests a synchronous transaction over key-value view.
     */
    @Test
    public void testTxSyncKeyValue() {
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
    public void testTxAsyncKeyValue() {
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

    @Test
    public void testSingleKeyCompositeTx() {
        igniteTransactions.runInTransaction(tx -> {
            Table txAcc = accounts.withTransaction(tx);

            Tuple current = txAcc.get(makeKey(1));

            txAcc.upsert(makeValue(1, current.doubleValue("balance") - DELTA));
        });

        // TODO tx is uncommited, read prev value.

        assertEquals(BALANCE_1, accounts.get(makeKey(1)).doubleValue("balance"));
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
