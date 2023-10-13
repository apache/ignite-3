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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for Table views.
 */
abstract class AbstractTableView {
    /** Internal table. */
    protected final InternalTable tbl;

    private final SchemaVersions schemaVersions;

    /** Table row view converter. */
    protected final TableViewRowConverter rowConverter;

    /**
     * Constructor.
     *
     * @param tbl Internal table.
     * @param schemaVersions Schema versions access.
     * @param schemaReg Schema registry.
     */
    AbstractTableView(InternalTable tbl, SchemaVersions schemaVersions, SchemaRegistry schemaReg) {
        this.tbl = tbl;
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
     * Executes the provided KV action in the given transaction, maintaining Schema Synchronization semantics: that is, before executing
     * the action, a check is made to make sure that the current node has schemas complete wrt timestamp corresponding to the operation
     * (if not, a wait is made till this condition is satisfied) and then the action is provided with the table schema version corresponding
     * to the operation.
     *
     * <p>If a transaction is given, the operation timestamp will be equal to the transaction timestamp; otherwise, 'now' will
     * be used.
     *
     * <p>If no transaction is provided, it might happen that schema version obtained at 'now' is different than a schema version
     * obtained when processing the implicit transaction (that will be created further). If this happens, we'll get an exception
     * saying that we need to make a retry; it will be handled by this method.
     *
     * @param <T> Type of the data the action returns.
     * @param tx Transaction or {@code null}.
     * @param action Action to execute.
     * @return Whatever the action returns.
     */
    protected final <T> CompletableFuture<T> withSchemaSync(@Nullable Transaction tx, KvAction<T> action) {
        // TODO: IGNITE-20106 - retry if our request is rejected by the server due to a changed schema version.

        CompletableFuture<Integer> schemaVersionFuture = tx == null
                ? schemaVersions.schemaVersionAtNow(tbl.tableId())
                : schemaVersions.schemaVersionAt(((InternalTransaction) tx).startTimestamp(), tbl.tableId());

        CompletableFuture<T> future = schemaVersionFuture.thenCompose(action::act);

        return convertToPublicFuture(future);
    }

    /**
     * Action representing some KV operation. When executed, the action is supplied with schema version corresponding
     * to the operation timestamp (see {@link #withSchemaSync(Transaction, KvAction)} for details).
     *
     * @param <R> Type of the result.
     * @see #withSchemaSync(Transaction, KvAction)
     */
    @FunctionalInterface
    protected interface KvAction<R> {
        /**
         * Executes the action.
         *
         * @param schemaVersion Schema version corresponding to the operation.
         * @return Action result.
         */
        CompletableFuture<R> act(int schemaVersion);
    }
}
