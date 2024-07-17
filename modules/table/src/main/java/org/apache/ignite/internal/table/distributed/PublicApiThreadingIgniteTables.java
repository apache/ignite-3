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

package org.apache.ignite.internal.table.distributed;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link IgniteTables} that maintains public API invariants relating to threading.
 * That is, it adds protection against thread hijacking by users and also marks threads as 'executing a sync user operation' or
 * 'executing an async user operation'.
 */
public class PublicApiThreadingIgniteTables implements IgniteTables, Wrapper {
    private final IgniteTables tables;
    private final Executor asyncContinuationExecutor;

    /**
     * Constructor.
     */
    public PublicApiThreadingIgniteTables(IgniteTables tables, Executor asyncContinuationExecutor) {
        assert !(tables instanceof Wrapper) : "Wrapping other wrappers is not supported";

        this.tables = tables;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public List<Table> tables() {
        return applyAntiHijackProtection(tables.tables());
    }

    private List<Table> applyAntiHijackProtection(List<Table> tablesToWrap) {
        return tablesToWrap.stream()
                .map(this::applyAntiHijackProtection)
                .collect(toList());
    }

    private @Nullable Table applyAntiHijackProtection(@Nullable Table table) {
        if (table == null) {
            return null;
        }

        return new PublicApiThreadingTable(table, asyncContinuationExecutor);
    }

    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        return preventThreadHijack(tables.tablesAsync())
                .thenApply(this::applyAntiHijackProtection);
    }

    @Override
    public Table table(String name) {
        return applyAntiHijackProtection(tables.table(name));
    }

    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        return preventThreadHijack(tables.tableAsync(name))
                .thenApply(this::applyAntiHijackProtection);
    }

    private <T> CompletableFuture<T> preventThreadHijack(CompletableFuture<T> originalFuture) {
        return PublicApiThreading.preventThreadHijack(originalFuture, asyncContinuationExecutor);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(tables);
    }
}
