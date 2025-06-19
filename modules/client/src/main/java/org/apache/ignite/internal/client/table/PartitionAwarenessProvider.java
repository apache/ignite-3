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

import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Partition awareness provider.
 * Used to calculate a partition for a specific operation.
 */
public class PartitionAwarenessProvider {
    static PartitionAwarenessProvider NULL_PROVIDER = of((Integer) null);

    private final @Nullable Integer partition;

    private final @Nullable Function<ClientSchema, Integer> hashFunc;

    private PartitionAwarenessProvider(@Nullable Function<ClientSchema, Integer> hashFunc, @Nullable Integer partition) {
        this.hashFunc = hashFunc;
        this.partition = partition;
    }

    public static PartitionAwarenessProvider of(Integer partition) {
        return new PartitionAwarenessProvider(null, partition);
    }

    public static PartitionAwarenessProvider of(Function<ClientSchema, Integer> hashFunc) {
        return new PartitionAwarenessProvider(hashFunc, null);
    }

    @Nullable Integer partition() {
        return partition;
    }

    @Nullable Integer getObjectHashCode(ClientSchema schema) {
        if (hashFunc == null) {
            return null;
        }

        return hashFunc.apply(schema);
    }
}
