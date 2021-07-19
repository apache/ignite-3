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
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for Table views.
 */
abstract class AbstractTableView {
    /** Internal table. */
    protected final InternalTable tbl;

    /** Schema registry. */
    protected final SchemaRegistry schemaReg;

    /** The transaction */
    protected final @Nullable Transaction tx;

    /**
     * Constructor
     * @param tbl Internal table.
     * @param schemaReg Schema registry.
     * @param tx The transaction.
     */
    protected AbstractTableView(InternalTable tbl, SchemaRegistry schemaReg, @Nullable Transaction tx) {
        this.tbl = tbl;
        this.schemaReg = schemaReg;
        this.tx = tx;
    }

    /**
     * @return Current transaction.
     */
    public @Nullable Transaction transaction() {
        return tx;
    }

    /**
     * @param tx The transaction.
     * @return Transactional view.
     */
    public abstract AbstractTableView withTransaction(Transaction tx);

    /**
     * Waits for operation completion.
     *
     * @param fut Future to wait to.
     * @param <T> Future result type.
     * @return Future result.
     */
    protected <T> T sync(CompletableFuture<T> fut) {
        try {
            return fut.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag.

            //TODO: IGNITE-14500 Replace with public exception with an error code.
            throw new IgniteInternalException(e);
        }
        catch (ExecutionException e) {
            //TODO: IGNITE-14500 Replace with public exception with an error code (or unwrap?).
            throw new IgniteInternalException(e);
        }
    }
}
