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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.table.PublicApiThreadingKeyValueView;
import org.apache.ignite.internal.table.PublicApiThreadingRecordView;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.Partition;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.PartitionManager;

/**
 * Wrapper around {@link Table} that maintains public API invariants relating to threading.
 * That is, it adds protection against thread hijacking by users and also marks threads as 'executing a sync user operation' or
 * 'executing an async user operation'.
 *
 * @see PublicApiThreading#preventThreadHijack(CompletableFuture, Executor)
 */
class PublicApiThreadingTable implements Table, Wrapper {
    private final Table table;
    private final Executor asyncContinuationExecutor;

    /**
     * Constructor.
     */
    PublicApiThreadingTable(Table table, Executor asyncContinuationExecutor) {
        this.table = table;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public String name() {
        return table.name();
    }

    @Override
    public <T extends Partition> PartitionManager<T> partitionManager() {
        return table.partitionManager();
    }

    @Override
    public <R> RecordView<R> recordView(Mapper<R> recMapper) {
        return new PublicApiThreadingRecordView<>(table.recordView(recMapper), asyncContinuationExecutor);
    }

    @Override
    public RecordView<Tuple> recordView() {
        return new PublicApiThreadingRecordView<>(table.recordView(), asyncContinuationExecutor);
    }

    @Override
    public <K, V> KeyValueView<K, V> keyValueView(Mapper<K> keyMapper, Mapper<V> valMapper) {
        return new PublicApiThreadingKeyValueView<>(table.keyValueView(keyMapper, valMapper), asyncContinuationExecutor);
    }

    @Override
    public KeyValueView<Tuple, Tuple> keyValueView() {
        return new PublicApiThreadingKeyValueView<>(table.keyValueView(), asyncContinuationExecutor);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(table);
    }
}
