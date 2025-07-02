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
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;

/** Stub implementation for {@link ExecutableTableRegistry}. */
public final class NoOpExecutableTableRegistry implements ExecutableTableRegistry {
    /** {@inheritDoc} */
    @Override
    public ExecutableTable getTable(int catalogVersion, int tableId) {
        return new NoOpExecutableTable(tableId);
    }

    private static final class NoOpExecutableTable implements ExecutableTable {

        private final int tableId;

        private NoOpExecutableTable(int tableId) {
            this.tableId = tableId;
        }

        /** {@inheritDoc} */
        @Override
        public ScannableTable scannableTable() {
            return new ScannableTable() {
                @Override
                public <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                        RowFactory<RowT> rowFactory, @Nullable ImmutableIntList requiredColumns) {
                    return SubscriptionUtils.fromIterable(new CompletableFuture<>());
                }

                @Override
                public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx,
                        PartitionWithConsistencyToken partWithConsistencyToken, RowFactory<RowT> rowFactory, int indexId,
                        List<String> columns, @Nullable RangeCondition<RowT> cond, @Nullable ImmutableIntList requiredColumns) {
                    return SubscriptionUtils.fromIterable(new CompletableFuture<>());
                }

                @Override
                public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx,
                        PartitionWithConsistencyToken partWithConsistencyToken, RowFactory<RowT> rowFactory, int indexId,
                        List<String> columns, RowT key, @Nullable ImmutableIntList requiredColumns) {
                    return SubscriptionUtils.fromIterable(new CompletableFuture<>());
                }

                @Override
                public <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(ExecutionContext<RowT> ctx,
                        @Nullable InternalTransaction explicitTx, RowFactory<RowT> rowFactory, RowT key,
                        @Nullable ImmutableIntList requiredColumns) {
                    return new CompletableFuture<>();
                }

                @Override
                public CompletableFuture<Long> estimatedSize() {
                    return new CompletableFuture<>();
                }
            };
        }

        /** {@inheritDoc} */
        @Override
        public UpdatableTable updatableTable() {
            return new UpdatableTable() {
                @Override
                public TableDescriptor descriptor() {
                    return null;
                }

                @Override
                public <RowT> CompletableFuture<?> insertAll(ExecutionContext<RowT> ectx, List<RowT> rows,
                        ColocationGroup colocationGroup) {
                    return new CompletableFuture<>();
                }

                @Override
                public <RowT> CompletableFuture<Void> insert(@Nullable InternalTransaction explicitTx, ExecutionContext<RowT> ectx,
                        RowT row) {
                    return new CompletableFuture<>();
                }

                @Override
                public <RowT> CompletableFuture<?> upsertAll(ExecutionContext<RowT> ectx, List<RowT> rows,
                        ColocationGroup colocationGroup) {
                    return null;
                }

                @Override
                public <RowT> CompletableFuture<?> deleteAll(ExecutionContext<RowT> ectx, List<RowT> rows,
                        ColocationGroup colocationGroup) {
                    return new CompletableFuture<>();
                }
            };
        }

        /** {@inheritDoc} */
        @Override
        public TableDescriptor tableDescriptor() {
            throw noDependency();
        }

        @Override
        public Supplier<PartitionCalculator> partitionCalculator() {
            throw new UnsupportedOperationException();
        }

        private IllegalStateException noDependency() {
            return new IllegalStateException("NoOpExecutableTable: " + tableId);
        }
    }
}
