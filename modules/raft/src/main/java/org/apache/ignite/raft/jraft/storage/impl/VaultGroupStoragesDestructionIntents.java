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

package org.apache.ignite.raft.jraft.storage.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.raft.jraft.storage.GroupStoragesDestructionIntents;

/** Uses VaultManager to destroy raft group storages durably, using vault to store destruction intents. */
public class VaultGroupStoragesDestructionIntents implements GroupStoragesDestructionIntents {
    private static final byte[] GROUP_STORAGE_DESTRUCTION_PREFIX = "destroy.group.storages.".getBytes(UTF_8);
    private static final ByteOrder BYTE_UTILS_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    private static final String PARTITION_GROUP_NAME = "partition";

    private static final int RAFT_GROUPS = 3;

    private final VaultManager vault;

    private final ConcurrentMap<String, RaftGroupOptionsConfigurer> configurerByName = new ConcurrentHashMap<>();

    /** Constructor. */
    public VaultGroupStoragesDestructionIntents(VaultManager vault) {
        this.vault = vault;
    }

    @Override
    public void addGroupOptionsConfigurer(ReplicationGroupId groupId, RaftGroupOptionsConfigurer groupOptionsConfigurer) {
        configurerByName.put(groupId.toString(), groupOptionsConfigurer);
    }

    @Override
    public void addPartitionGroupOptionsConfigurer(RaftGroupOptionsConfigurer groupOptionsConfigurer) {
        configurerByName.put(PARTITION_GROUP_NAME, groupOptionsConfigurer);
    }

    @Override
    public void saveDestroyStorageIntent(RaftNodeId nodeId, RaftGroupOptions groupOptions) {
        String configurerName = nodeId.groupId() instanceof PartitionGroupId ? PARTITION_GROUP_NAME : nodeId.groupId().toString();

        vault.put(
                buildKey(nodeId.nodeIdStringForStorage()),
                toBytes(new DestroyStorageIntent(configurerName, groupOptions.volatileStores()))
        );
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

    private static String raftNodeIdFromKey(byte[] key) {
        return new String(key, GROUP_STORAGE_DESTRUCTION_PREFIX.length, key.length - GROUP_STORAGE_DESTRUCTION_PREFIX.length, UTF_8);
    }

    @Override
    public Map<String, RaftGroupOptions> readGroupOptionsByNodeIdForDestruction() {
        assert configurerByName.size() == RAFT_GROUPS
                : "Configurers for CMG, metastorage and partitions must be added, got: " + configurerByName.keySet();

        try (Cursor<VaultEntry> cursor = vault.prefix(new ByteArray(GROUP_STORAGE_DESTRUCTION_PREFIX))) {
            Map<String, RaftGroupOptions> result = new HashMap<>();

            while (cursor.hasNext()) {
                VaultEntry next = cursor.next();

                String nodeId = raftNodeIdFromKey(next.key().bytes());

                // todo add serializer
                DestroyStorageIntent intent = ByteUtils.fromBytes(next.value());

                RaftGroupOptions groupOptions = intent.isVolatile
                        ? RaftGroupOptions.defaults()
                        : RaftGroupOptions.forPersistentStores();

                configurerByName.get(intent.configurerName).configure(groupOptions);

                result.put(nodeId, groupOptions);
            }

            return result;
        }
    }

    private static class DestroyStorageIntent {
        final boolean isVolatile;

        final String configurerName;

        private DestroyStorageIntent(String configurerName, boolean isVolatile) {
            this.configurerName = configurerName;
            this.isVolatile = isVolatile;
        }
    }
}
