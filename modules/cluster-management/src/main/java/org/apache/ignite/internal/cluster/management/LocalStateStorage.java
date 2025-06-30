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

package org.apache.ignite.internal.cluster.management;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * Class that represents a local CMG state (persisted in the Vault).
 *
 * <p>This state has nothing to do with local Raft state and is only used on node startup, when Raft groups have not yet been started.
 */
class LocalStateStorage {
    private static final ByteArray CMG_STATE_VAULT_KEY = ByteArray.fromString("cmg_state");

    static class LocalState {
        private final Set<String> cmgNodeNames;

        @IgniteToStringInclude
        private final ClusterTag clusterTag;

        LocalState(Set<String> cmgNodeNames, ClusterTag clusterTag) {
            this.cmgNodeNames = Set.copyOf(cmgNodeNames);
            this.clusterTag = clusterTag;
        }

        Set<String> cmgNodeNames() {
            return cmgNodeNames;
        }

        ClusterTag clusterTag() {
            return clusterTag;
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    private final VaultManager vault;

    LocalStateStorage(VaultManager vault) {
        this.vault = vault;
    }

    private static final IgniteLogger LOG = Loggers.forClass(LocalStateStorage.class);

    /**
     * Retrieves the local state.
     *
     * @return Local state.
     */
    @Nullable LocalState getLocalState() {
        LocalState localState0 = getLocalState0();

        LOG.info(">>>>> LocalStateStorage#getLocalState: {}", localState0);

        return localState0;
    }

    private @Nullable LocalState getLocalState0() {
        VaultEntry entry = vault.get(CMG_STATE_VAULT_KEY);

        if (entry == null) {
            return null;
        }

        return VersionedSerialization.fromBytes(entry.value(), LocalStateSerializer.INSTANCE);
    }

    /**
     * Saves a given local state.
     *
     * @param state Local state to save.
     */
    void saveLocalState(LocalState state) {
        vault.put(CMG_STATE_VAULT_KEY, VersionedSerialization.toBytes(state, LocalStateSerializer.INSTANCE));

        LOG.info(">>>>> LocalStateStorage#saveLocalState: {}", new Exception(), state);
    }

    /**
     * Removes all data from the local storage.
     */
    void clear() {
        vault.remove(CMG_STATE_VAULT_KEY);

        LOG.info(">>>>> LocalStateStorage#clear");
    }

    private static class LocalStateSerializer extends VersionedSerializer<LocalState> {
        private static final CmgMessagesFactory CMG_MESSAGES_FACTORY = new CmgMessagesFactory();

        private static final LocalStateSerializer INSTANCE = new LocalStateSerializer();

        @Override
        protected void writeExternalData(LocalState state, IgniteDataOutput out) throws IOException {
            out.writeVarInt(state.cmgNodeNames().size());
            for (String cmgNodeName : state.cmgNodeNames()) {
                out.writeUTF(cmgNodeName);
            }

            out.writeUTF(state.clusterTag().clusterName());
            out.writeUuid(state.clusterTag().clusterId());
        }

        @Override
        protected LocalState readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
            int cmgNodesCount = in.readVarIntAsInt();
            Set<String> cmgNodeNames = new HashSet<>(cmgNodesCount);
            for (int i = 0; i < cmgNodesCount; i++) {
                cmgNodeNames.add(in.readUTF());
            }

            ClusterTag clusterTag = ClusterTag.clusterTag(CMG_MESSAGES_FACTORY, in.readUTF(), in.readUuid());

            return new LocalState(cmgNodeNames, clusterTag);
        }
    }
}
