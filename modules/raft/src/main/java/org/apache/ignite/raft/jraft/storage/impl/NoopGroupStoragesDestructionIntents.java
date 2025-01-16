package org.apache.ignite.raft.jraft.storage.impl;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.raft.jraft.storage.GroupStoragesDestructionIntents;

// Storage that doesn't save intents to destroy group storages.
public class NoopGroupStoragesDestructionIntents implements GroupStoragesDestructionIntents {
    @Override
    public void addGroupOptionsConfigurer(ReplicationGroupId groupId, RaftGroupOptionsConfigurer groupOptionsConfigurer) {
        // No-op.
    }

    @Override
    public void addPartitionGroupOptionsConfigurer(RaftGroupOptionsConfigurer partitionRaftConfigurer) {
        // No-op.
    }

    @Override
    public void saveDestroyStorageIntent(RaftNodeId nodeId, RaftGroupOptions groupOptions) {
        // No-op.
    }

    @Override
    public void removeDestroyStorageIntent(String nodeId) {
        // No-op.
    }

    @Override
    public Map<String, RaftGroupOptions> readGroupOptionsByNodeIdForDestruction() {
        return Collections.emptyMap();
    }
}
