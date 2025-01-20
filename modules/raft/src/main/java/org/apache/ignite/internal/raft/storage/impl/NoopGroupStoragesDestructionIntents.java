package org.apache.ignite.internal.raft.storage.impl;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.raft.storage.GroupStoragesDestructionIntents;
import org.apache.ignite.internal.raft.storage.impl.VaultGroupStoragesDestructionIntents.DestroyStorageIntent;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

// Storage that doesn't save intents to destroy group storages.
public class NoopGroupStoragesDestructionIntents implements GroupStoragesDestructionIntents {
    @Override
    public void saveDestroyStorageIntent(ReplicationGroupId groupId, DestroyStorageIntent intent) {
        // No-op.
    }

    @Override
    public void removeDestroyStorageIntent(String nodeId) {
        // No-op.
    }

    @Override
    public Collection<DestroyStorageIntent> readDestroyStorageIntentsByName() {
        return Collections.emptyList();
    }
}
