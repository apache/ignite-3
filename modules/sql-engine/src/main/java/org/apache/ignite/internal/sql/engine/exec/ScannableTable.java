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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.jetbrains.annotations.Nullable;

/**
 * Provides read operations on a table.
 */
public interface ScannableTable {

    /**
     * Performs a scan over table.
     *
     * @param ctx  Execution context.
     * @param partWithConsistencyToken  Partition.
     * @param rowFactory  Row factory.
     * @param requiredColumns  Required columns.
     * @return  A publisher that produces rows.
     * @param <RowT>  A type of row.
     */
    <RowT> Publisher<RowT> scan(
            ExecutionContext<RowT> ctx,
            PartitionWithConsistencyToken partWithConsistencyToken,
            RowFactory<RowT> rowFactory,
            @Nullable BitSet requiredColumns
    );

    /**
     * Performs range scan using the given index.
     *
     * @param <RowT> A type of row.
     * @param ctx Execution context.
     * @param partWithConsistencyToken Partition.
     * @param rowFactory Row factory.
     * @param indexId Index id.
     * @param columns Index columns.
     * @param cond Index condition.
     * @param requiredColumns Required columns.
     * @return A publisher that produces rows.
     */
    <RowT> Publisher<RowT> indexRangeScan(
            ExecutionContext<RowT> ctx,
            PartitionWithConsistencyToken partWithConsistencyToken,
            RowFactory<RowT> rowFactory,
            int indexId,
            List<String> columns,
            @Nullable RangeCondition<RowT> cond,
            @Nullable BitSet requiredColumns
    );

    /**
     * Performs a lookup scan using the given index.
     *
     * @param <RowT> A type of row.
     * @param ctx Execution context.
     * @param partWithConsistencyToken Partition.
     * @param rowFactory Row factory.
     * @param indexId Index id.
     * @param columns Index columns.
     * @param key A key to lookup.
     * @param requiredColumns Required columns.
     * @return A publisher that produces rows.
     */
    <RowT> Publisher<RowT> indexLookup(
            ExecutionContext<RowT> ctx,
            PartitionWithConsistencyToken partWithConsistencyToken,
            RowFactory<RowT> rowFactory,
            int indexId,
            List<String> columns,
            RowT key,
            @Nullable BitSet requiredColumns
    );

    /**
     * Performs a lookup by primary index.
     *
     * <p>Note: this scan may be performed on initiator node only since it requires an
     * original transaction rather than attributes, and transaction is only available on
     * initiator node.
     *
     * @param <RowT> A type of row.
     * @param ctx Execution context.
     * @param tx Transaction to use to perform lookup.
     * @param rowFactory Row factory.
     * @param key A key to lookup.
     * @param requiredColumns Required columns.
     * @return A future representing result of operation.
     */
    <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(
            ExecutionContext<RowT> ctx,
            InternalTransaction tx,
            RowFactory<RowT> rowFactory,
            RowT key,
            @Nullable BitSet requiredColumns
    );
}
