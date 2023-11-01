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

package org.apache.ignite.internal.table.distributed.storage;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

/**
 * Batch of binary rows from the original collection of binary rows for processing, preserving the order from the original collection.
 *
 * <p>NOTE: Not thread-safe.
 */
public class RowBatch {
    /** Batch of rows from the original collection of rows. */
    public final List<BinaryRow> requestedRows = new ArrayList<>();

    /** Order of the rows from the {@link #requestedRows} in the original row collection. */
    final IntList originalRowOrder = new IntArrayList();

    /**
     * Future of the result of processing the {@link #requestedRows}, {@code null} if not set and may return {@code null}.
     */
    public @Nullable CompletableFuture<?> resultFuture;

    public void add(BinaryRow row, int originalIndex) {
        requestedRows.add(row);
        originalRowOrder.add(originalIndex);
    }

    @Nullable Object getCompletedResult() {
        CompletableFuture<?> resultFuture = this.resultFuture;

        assert resultFuture != null;
        assert resultFuture.isDone();

        return resultFuture.join();
    }

    int getOriginalRowIndex(int resultRowIndex) {
        return originalRowOrder.getInt(resultRowIndex);
    }

    static CompletableFuture<Void> allResultFutures(Collection<RowBatch> batches) {
        return CompletableFuture.allOf(batches.stream().map(rowBatch -> rowBatch.resultFuture).toArray(CompletableFuture[]::new));
    }

    static int getTotalRequestedRowSize(Collection<RowBatch> batches) {
        int totalSize = 0;

        for (RowBatch batch : batches) {
            totalSize += batch.requestedRows.size();
        }

        return totalSize;
    }
}
