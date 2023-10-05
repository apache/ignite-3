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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.convertToPublicFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for Table views.
 */
abstract class AbstractTableView {
    /** Internal table. */
    protected final InternalTable tbl;

    private final TxManager txManager;

    private final HybridTimestampTracker observableTimestampTracker;

    private final SchemaVersions schemaVersions;

    /** Table row view converter. */
    protected final TableViewRowConverter rowConverter;

    /**
     * Constructor.
     *
     * @param tbl Internal table.
     * @param txManager Transaction manager.
     * @param observableTimestampTracker Timestamp tracker to use when creating implicit transactions.
     * @param schemaVersions Schema versions access.
     * @param schemaReg Schema registry.
     */
    AbstractTableView(
            InternalTable tbl,
            TxManager txManager,
            HybridTimestampTracker observableTimestampTracker,
            SchemaVersions schemaVersions,
            SchemaRegistry schemaReg
    ) {
        this.tbl = tbl;
        this.txManager = txManager;
        this.observableTimestampTracker = observableTimestampTracker;
        this.schemaVersions = schemaVersions;

        this.rowConverter = new TableViewRowConverter(schemaReg);
    }

    /**
     * Waits for operation completion.
     *
     * @param fut Future to wait to.
     * @param <T> Future result type.
     * @return Future result.
     */
    protected final <T> T sync(CompletableFuture<T> fut) {
        try {
            return fut.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag.

            throw sneakyThrow(IgniteExceptionMapperUtil.mapToPublicException(e));
        } catch (ExecutionException e) {
            Throwable cause = ExceptionUtils.unwrapCause(e);
            throw sneakyThrow(cause);
        }
    }

    /**
     * Executes the provided KV action in the given transaction or, if {@code null} is given instead of a transaction,
     * in an implicit transaction, maintaining Schema Synchronization semantics: that is, before executing
     * the action, a check is made to make sure that the current node has schemas complete wrt timestamp
     * corresponding to the transaction (if not, a wait is made till this condition is satisfied) and then
     * the action is provided with the table schema version corresponding to the transaction.
     *
     * @param tx Transaction or {@code null}.
     * @param action Action to execute.
     * @param <T> Type of the data the action returns.
     * @return Whatever the action returns.
     */
    protected final <T> CompletableFuture<T> withSchemaSync(@Nullable Transaction tx, KvAction<T> action) {
        return withSchemaSync(tx, false, action);
    }

    /**
     * Executes the provided KV action in the given transaction or, if {@code null} is given instead of a transaction,
     * in an implicit transaction, maintaining Schema Synchronization semantics: that is, before executing
     * the action, a check is made to make sure that the current node has schemas complete wrt timestamp
     * corresponding to the transaction (if not, a wait is made till this condition is satisfied) and then
     * the action is provided with the table schema version corresponding to the transaction.
     *
     * @param tx Transaction or {@code null}.
     * @param startRoTx Whether an implicit TX (if it's needed) should be RO.
     * @param action Action to execute.
     * @param <T> Type of the data the action returns.
     * @return Whatever the action returns.
     */
    protected final <T> CompletableFuture<T> withSchemaSync(@Nullable Transaction tx, boolean startRoTx, KvAction<T> action) {
        InternalTransaction actualTx = startImplicitTxIfNeeded(tx, startRoTx);
        boolean weStartedImplicitTx = actualTx != tx;

        boolean operationMightBeExecuted = false;

        try {
            CompletableFuture<T> future = schemaVersionFor(actualTx)
                    .thenCompose(schemaVersion -> action.act(actualTx, schemaVersion))
                    .whenComplete((res, ex) -> eraseTxMetaAfterActionIfNeeded(actualTx, ex, weStartedImplicitTx));

            operationMightBeExecuted = true;

            return convertToPublicFuture(future);
        } finally {
            if (weStartedImplicitTx && !operationMightBeExecuted) {
                // Something went wrong before we even initiated an operation, cleanup.
                eraseTxMeta(actualTx);
            }
        }
    }

    private InternalTransaction startImplicitTxIfNeeded(@Nullable Transaction tx, boolean startRoTx) {
        return tx == null ? txManager.beginImplicit(observableTimestampTracker, startRoTx) : (InternalTransaction) tx;
    }

    private CompletableFuture<Integer> schemaVersionFor(InternalTransaction actualTx) {
        return schemaVersions.schemaVersionAt(actualTx.startTimestamp(), tbl.tableId());
    }

    private void eraseTxMetaAfterActionIfNeeded(InternalTransaction actualTx, Throwable ex, boolean weStartedImplicitTx) {
        TxStateMeta txStateMeta = txManager.stateMeta(actualTx.id());

        if (weStartedImplicitTx && txStateMeta != null && txStateMeta.txState() == TxState.PENDING) {
            // InternalTable did not handle the transaction, maybe an exception happened before an InternalTable method
            // was called.
            if (ex != null) {
                eraseTxMeta(actualTx);
            }
        }
    }

    private void eraseTxMeta(InternalTransaction actualTx) {
        txManager.updateTxMeta(actualTx.id(), oldMeta -> null);
    }

    /**
     * Determines whether rows belong to the same partition.
     *
     * @param rows Rows batch.
     */
    protected final boolean isSinglePartitionBatch(Collection<BinaryRowEx> rows) {
        if (rows.isEmpty()) {
            return true;
        }

        Iterator<BinaryRowEx> rowIterator = rows.iterator();

        int partId = tbl.partitionId(rowIterator.next());

        while (rowIterator.hasNext()) {
            BinaryRowEx row = rowIterator.next();

            if (partId != tbl.partitionId(row)) {
                return false;
            }
        }

        return true;
    }
}
