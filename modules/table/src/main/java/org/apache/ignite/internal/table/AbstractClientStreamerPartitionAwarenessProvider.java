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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.streamer.StreamerPartitionAwarenessProvider;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Partition awareness provider for data streamer.
 *
 * @param <T> Item type.
 */
abstract class AbstractClientStreamerPartitionAwarenessProvider<T> implements StreamerPartitionAwarenessProvider<T, Integer> {
    private final SchemaRegistry schemaReg;

    private final int partitions;

    AbstractClientStreamerPartitionAwarenessProvider(SchemaRegistry schemaReg, int partitions) {
        assert schemaReg != null;
        this.schemaReg = schemaReg;
        this.partitions = partitions;
    }

    @Override
    public Integer partition(T item) {
        var colocationHash = colocationHash(schemaReg.lastKnownSchema(), item);
        return IgniteUtils.safeAbs(colocationHash) % partitions;
    }

    abstract int colocationHash(SchemaDescriptor schema, T item);

    @Override
    public CompletableFuture<Void> refreshAsync() {
        return nullCompletedFuture();
    }
}
