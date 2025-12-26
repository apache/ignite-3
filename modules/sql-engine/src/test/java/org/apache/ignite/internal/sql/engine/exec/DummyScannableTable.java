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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;

/** Test implementation of {@link ScannableTable}. It does not perform any real operations. */
class DummyScannableTable implements ScannableTable {
    @Override
    public <RowT> Publisher<RowT> scan(
            ExecutionContext<RowT> ctx,
            PartitionWithConsistencyToken partWithConsistencyToken,
            RowFactory<RowT> rowFactory,
            int @Nullable [] requiredColumns
    ) {
        return SubscriptionUtils.fromIterable(new CompletableFuture<>());
    }

    @Override
    public <RowT> Publisher<RowT> indexRangeScan(
            ExecutionContext<RowT> ctx,
            PartitionWithConsistencyToken partWithConsistencyToken,
            RowFactory<RowT> rowFactory,
            int indexId,
            List<String> columns,
            @Nullable RangeCondition<RowT> cond,
            int @Nullable [] requiredColumns
    ) {
        return SubscriptionUtils.fromIterable(new CompletableFuture<>());
    }

    @Override
    public <RowT> Publisher<RowT> indexLookup(
            ExecutionContext<RowT> ctx,
            PartitionWithConsistencyToken partWithConsistencyToken,
            RowFactory<RowT> rowFactory,
            int indexId,
            List<String> columns,
            RowT key,
            int @Nullable [] requiredColumns
    ) {
        return SubscriptionUtils.fromIterable(new CompletableFuture<>());
    }

    @Override
    public <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(
            ExecutionContext<RowT> ctx,
            @Nullable InternalTransaction explicitTx,
            RowFactory<RowT> rowFactory,
            RowT key,
            int @Nullable [] requiredColumns
    ) {
        return new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<Long> estimatedSize() {
        return new CompletableFuture<>();
    }
}
