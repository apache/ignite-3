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
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.jetbrains.annotations.Nullable;

/** Test implementation of {@link ScannableTable}. It does not perform any real operations. */
class DummyUpdatableTable implements UpdatableTable {
    @Override
    public TableDescriptor descriptor() {
        throw new UnsupportedOperationException("UpdatableTable#descriptor");
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
    public <RowT> CompletableFuture<Boolean> delete(@Nullable InternalTransaction explicitTx, ExecutionContext<RowT> ectx, RowT key) {
        return new CompletableFuture<>();
    }

    @Override
    public <RowT> CompletableFuture<?> deleteAll(ExecutionContext<RowT> ectx, List<RowT> rows,
            ColocationGroup colocationGroup) {
        return new CompletableFuture<>();
    }
}
