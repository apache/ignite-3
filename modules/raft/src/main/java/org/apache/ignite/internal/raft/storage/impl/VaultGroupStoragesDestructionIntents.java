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
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.raft.storage.GroupStoragesDestructionIntents;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;

/** Uses VaultManager to store destruction intents. */
public class VaultGroupStoragesDestructionIntents implements GroupStoragesDestructionIntents {
    /** Initial capacity (in bytes) of the buffer used for data output. */
    private static final int INITIAL_BUFFER_CAPACITY = 64;

    private static final byte[] GROUP_STORAGE_DESTRUCTION_PREFIX = "destroy.group.storages.".getBytes(UTF_8);
    private static final ByteOrder BYTE_UTILS_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    private final VaultManager vault;

    /** Constructor. */
    public VaultGroupStoragesDestructionIntents(VaultManager vault) {
        this.vault = vault;
    }

    @Override
    public void saveDestroyStorageIntent(ReplicationGroupId groupId, DestroyStorageIntent destroyStorageIntent) {
        vault.put(buildKey(destroyStorageIntent.nodeId()), toStorageBytes(destroyStorageIntent));
    }

    private static byte[] toStorageBytes(DestroyStorageIntent intent) {
        try (IgniteUnsafeDataOutput out = new IgniteUnsafeDataOutput(INITIAL_BUFFER_CAPACITY)) {
            out.writeUTF(intent.groupName());
            out.writeBoolean(intent.isVolatile());

            return out.array();
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Cannot serialize", e);
        }
    }

    private static DestroyStorageIntent fromStorageBytes(byte[] key, byte[] value) {
        String nodeId = nodeIdFromKey(key);

        try (IgniteUnsafeDataInput in = new IgniteUnsafeDataInput(value)) {
            String groupName = in.readUTF();
            boolean isVolatile = in.readBoolean();

            DestroyStorageIntent intent = new DestroyStorageIntent(
                    nodeId,
                    groupName,
                    isVolatile
            );

            if (in.available() != 0) {
                throw new IOException(in.available() + " bytes left unread after deserializing " + intent);
            }

            return intent;
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Cannot deserialize", e);
        }
    }

    private static String nodeIdFromKey(byte[] key) {
        return new String(key, GROUP_STORAGE_DESTRUCTION_PREFIX.length, key.length - GROUP_STORAGE_DESTRUCTION_PREFIX.length, UTF_8);
    }

    @Override
    public void removeDestroyStorageIntent(String nodeId) {
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
    public Collection<DestroyStorageIntent> readDestroyStorageIntents() {
        try (Cursor<VaultEntry> cursor = vault.prefix(new ByteArray(GROUP_STORAGE_DESTRUCTION_PREFIX))) {
            Collection<DestroyStorageIntent> result = new ArrayList<>();

            while (cursor.hasNext()) {
                VaultEntry next = cursor.next();

                DestroyStorageIntent intent = fromStorageBytes(next.key().bytes(), next.value());

                result.add(intent);
            }

            return result;
        }
    }
}
