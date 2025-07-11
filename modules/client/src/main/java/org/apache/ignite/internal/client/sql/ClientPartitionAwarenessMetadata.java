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

package org.apache.ignite.internal.client.sql;

import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;

/**
 * Partition awareness metadata.
 *
 * <p>The {@code indexes} array is used to describe how each element of a colocation
 * key should be interpreted during evaluation:
 * <ul>
 *   <li>If {@code indexes[i] >= 0}, then the value at position {@code i} in the
 *   colocation key should be taken from a dynamic parameter at index {@code indexes[i]}.</li>
 *   <li>If {@code indexes[i] < 0}, then the value at position {@code i} is a constant
 *   literal whose precomputed hash is stored in the {@code hash} array.
 *   The corresponding hash value is located at index {@code -(indexes[i] + 1)} in the
 *   {@code hash} array.</li>
 * </ul>
 * In other words:
 * <pre>
 *   indexes[i] >= 0         => use dynamicParam[indexes[i]]
 *   indexes[i] < 0          => use hash[-(indexes[i] + 1)]
 * </pre>
 */
public final class ClientPartitionAwarenessMetadata {
    private final int tableId;
    private final int[] indexes;
    private final int[] hash;
    private final ClientDirectTxMode directTxMode;

    private ClientPartitionAwarenessMetadata(int tableId, int[] indexes, int[] hash, ClientDirectTxMode directTxMode) {
        this.tableId = tableId;
        this.indexes = indexes;
        this.hash = hash;
        this.directTxMode = directTxMode;
    }

    static ClientPartitionAwarenessMetadata read(ClientMessageUnpacker unpacker, boolean sqlDirectMappingSupported) {
        int tableId = unpacker.unpackInt();
        int[] indexes = unpacker.unpackIntArray();
        int[] hash = unpacker.unpackIntArray();

        ClientDirectTxMode directTxMode = ClientDirectTxMode.NOT_SUPPORTED;
        if (sqlDirectMappingSupported) {
            directTxMode = ClientDirectTxMode.fromId(unpacker.unpackByte());
        }

        return new ClientPartitionAwarenessMetadata(tableId, indexes, hash, directTxMode);
    }

    public int tableId() {
        return tableId;
    }

    public int[] indexes() {
        return indexes;
    }

    public int[] hash() {
        return hash;
    }

    ClientDirectTxMode directTxMode() {
        return directTxMode;
    }
}
