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

package org.apache.ignite.internal.raft.storage.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.raft.storage.GroupStoragesDestructionIntents;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.versioned.VersionedSerialization;

/** Uses VaultManager to store destruction intents. */
public class VaultGroupStoragesDestructionIntents implements GroupStoragesDestructionIntents {
    private static final byte[] GROUP_STORAGE_DESTRUCTION_PREFIX = "destroy.group.storages.".getBytes(UTF_8);
    private static final ByteOrder BYTE_UTILS_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    private final VaultManager vault;

    /** Constructor. */
    public VaultGroupStoragesDestructionIntents(VaultManager vault) {
        this.vault = vault;
    }

    @Override
    public void saveStorageDestructionIntent(StorageDestructionIntent storageDestructionIntent) {
        vault.put(
                buildKey(storageDestructionIntent.nodeId()),
                VersionedSerialization.toBytes(storageDestructionIntent, StorageDestructionIntentSerializer.INSTANCE)
        );
    }

    @Override
    public void removeStorageDestructionIntent(String nodeId) {
        vault.remove(buildKey(nodeId));
    }

    private static ByteArray buildKey(String nodeId) {
        byte[] nodeIdBytes = nodeId.getBytes(UTF_8);

        byte[] key = ByteBuffer.allocate(GROUP_STORAGE_DESTRUCTION_PREFIX.length + nodeIdBytes.length)
                .order(BYTE_UTILS_BYTE_ORDER)
                .put(GROUP_STORAGE_DESTRUCTION_PREFIX)
                .put(nodeIdBytes)
                .array();

        return new ByteArray(key);
    }

    @Override
    public Collection<StorageDestructionIntent> readStorageDestructionIntents() {
        try (Cursor<VaultEntry> cursor = vault.prefix(new ByteArray(GROUP_STORAGE_DESTRUCTION_PREFIX))) {
            Collection<StorageDestructionIntent> result = new ArrayList<>();

            while (cursor.hasNext()) {
                VaultEntry next = cursor.next();

                StorageDestructionIntent intent = VersionedSerialization.fromBytes(
                        next.value(),
                        StorageDestructionIntentSerializer.INSTANCE
                );

                result.add(intent);
            }

            return result;
        }
    }
}
