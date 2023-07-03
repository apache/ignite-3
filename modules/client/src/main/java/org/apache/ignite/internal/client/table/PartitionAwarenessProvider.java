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
 * Represents 3 use cases:
 * 1. Partition awareness is enabled. Use hashFunc to determine partition.
 * 2. Transaction is used. Use specific channel.
 * 3. Null instance = No partition awareness and no transaction. Use any channel.
 */
class PartitionAwarenessProvider {
    private final @Nullable String nodeName;

    private final @Nullable Function<ClientSchema, Integer> hashFunc;

    private PartitionAwarenessProvider(@Nullable String nodeName, @Nullable Function<ClientSchema, Integer> hashFunc) {
        assert (nodeName == null) ^ (hashFunc == null) : "One must be null, another not null: nodeId, hashFunc";

        this.nodeName = nodeName;
        this.hashFunc = hashFunc;
    }

    public static PartitionAwarenessProvider of(String nodeName) {
        return new PartitionAwarenessProvider(nodeName, null);
    }

    public static PartitionAwarenessProvider of(Function<ClientSchema, Integer> hashFunc) {
        return new PartitionAwarenessProvider(null, hashFunc);
    }

    @Nullable String nodeName() {
        return nodeName;
    }

    Integer getObjectHashCode(ClientSchema schema) {
        if (hashFunc == null) {
            throw new IllegalStateException("Partition awareness is not enabled. Check channel() first.");
        }

        return hashFunc.apply(schema);
    }

    boolean isPartitionAwarenessEnabled() {
        return hashFunc != null;
    }
}
