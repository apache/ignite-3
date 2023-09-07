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

import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteExceptionMapperUtil;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper around {@link InternalTable} that converts internal exceptions to public exceptions.
 * Methods of {@link InternalTable} that could throw internal exceptions should be called via this wrapper.
 */
final class TableOperations {

    private final InternalTable table;

    TableOperations(InternalTable table) {
        this.table = table;
    }

    CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.get(keyRow, tx));
    }

    CompletableFuture<List<BinaryRow>> getAll(Collection<BinaryRowEx> keyRows, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.getAll(keyRows, tx));
    }

    CompletableFuture<Void> upsert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.upsert(row, tx));
    }

    CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.upsertAll(rows, tx));
    }

    CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, int partition) {
        return convertExceptionAsync(table.upsertAll(rows, partition));
    }

    CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.getAndUpsert(row, tx));
    }

    CompletableFuture<Boolean> insert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.insert(row, tx));
    }

    CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.insertAll(rows, tx));
    }

    CompletableFuture<Boolean> replace(BinaryRowEx row, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.replace(row, tx));
    }

    CompletableFuture<Boolean> replace(BinaryRowEx oldRow, BinaryRowEx newRow, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.replace(oldRow, newRow, tx));
    }

    CompletableFuture<BinaryRow> getAndReplace(BinaryRowEx row, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.getAndReplace(row, tx));
    }

    CompletableFuture<Boolean> delete(BinaryRowEx keyRow, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.delete(keyRow, tx));
    }

    CompletableFuture<Boolean> deleteExact(BinaryRowEx oldRow, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.deleteExact(oldRow, tx));
    }

    CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.getAndDelete(row, tx));
    }

    CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.deleteAll(rows, tx));
    }

    CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        return convertExceptionAsync(table.deleteAllExact(rows, tx));
    }

    /**
     * Returns underlying {@link InternalTable}.
     */
    public InternalTable internalTable() {
        return table;
    }

    private static  <T> CompletableFuture<T> convertExceptionAsync(CompletionStage<T> f) {
        return IgniteExceptionMapperUtil.convertToPublicFuture(f.toCompletableFuture());
    }
}
