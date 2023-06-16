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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.streamer.StreamerPartitionAwarenessProvider;

/**
 * Partition awareness provider for data streamer.
 *
 * @param <T> Item type.
 */
abstract class AbstractClientStreamerPartitionAwarenessProvider<T> implements StreamerPartitionAwarenessProvider<T, Integer> {
    private static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    private final SchemaRegistry schemaReg;

    AbstractClientStreamerPartitionAwarenessProvider(SchemaRegistry schemaReg) {
        this.schemaReg = schemaReg;
    }

    @Override
    public Integer partition(T item) {
        return colocationHash(schemaReg.schema(), item);
    }

    abstract int colocationHash(SchemaDescriptor schema, T item);

    @Override
    public CompletableFuture<Void> refreshAsync() {
        return COMPLETED_FUTURE;
    }
}
