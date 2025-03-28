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

package org.apache.ignite.internal.client.table;

import java.util.function.BiFunction;
import org.jetbrains.annotations.Nullable;

/**
 * Partition awareness provider.
 * Represents 3 use cases:
 * 1. Partition awareness is enabled. Use hashFunc to determine partition.
 * 2. Transaction is used. Use specific channel.
 * 3. Null instance = No partition awareness and no transaction. Use any channel.
 */
public class PartitionAwarenessProvider {
    private final @Nullable Integer partition;

    private final @Nullable BiFunction<ClientSchema, Boolean, Integer> hashFunc;

    private PartitionAwarenessProvider(@Nullable BiFunction<ClientSchema, Boolean, Integer> hashFunc, @Nullable Integer partition) {
        assert hashFunc != null ^ partition != null;

        this.hashFunc = hashFunc;
        this.partition = partition;
    }

    public static PartitionAwarenessProvider of(Integer partition) {
        return new PartitionAwarenessProvider(null, partition);
    }

    public static PartitionAwarenessProvider of(BiFunction<ClientSchema, Boolean, Integer> hashFunc) {
        return new PartitionAwarenessProvider(hashFunc, null);
    }

    @Nullable Integer partition() {
        return partition;
    }

    @Nullable Integer getObjectHashCode(ClientSchema schema, boolean coord) {
        if (hashFunc == null) {
            throw new IllegalStateException("Partition awareness is not enabled. Check channel() first.");
        }

        return hashFunc.apply(schema, coord);
    }
}
