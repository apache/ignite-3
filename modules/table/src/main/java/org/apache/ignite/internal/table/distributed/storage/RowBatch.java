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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

/**
 * Batch of binary rows from the original collection of binary rows for processing, preserving the order from the original collection.
 *
 * <p>NOTE: Not thread-safe.
 */
class RowBatch {
    /** Batch of rows from the original collection of rows. */
    final List<BinaryRow> rows = new ArrayList<>();

    /** Order of the rows from the {@link #rows} in the original row collection. */
    final IntList originalRowOrder = new IntArrayList();

    /**
     * Future of the result of processing the {@link #rows}, {@code null} if not set and may return {@code null}.
     */
    @Nullable CompletableFuture<Object> resultFuture;

    void add(BinaryRow row, int originalIndex) {
        rows.add(row);
        originalRowOrder.add(originalIndex);
    }
}
