package org.apache.ignite.internal.raft.storage.impl;


import java.nio.file.Path;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.tostring.S;

/** Intent to destroy raft node's group storages. */
public class DestroyStorageIntent {
    private final boolean isVolatile;
    private final String raftNodeId;
    private final String groupName;

    /** Constructor. */
    public DestroyStorageIntent(String raftNodeId, String groupName, boolean isVolatile) {
        this.raftNodeId = raftNodeId;
        this.groupName = groupName;
        this.isVolatile = isVolatile;
    }

    public String nodeId() {
        return raftNodeId;
    }

    public boolean isVolatile() {
        return isVolatile;
    }

    @Override
    public String toString() {
        return S.toString(DestroyStorageIntent.class, this);
    }

    public String groupName() {
        return groupName;
    }
}