package org.apache.ignite.internal.raft.storage.impl;


import java.nio.file.Path;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.tostring.S;

/** Intent to destroy raft node's group storages. */
public class DestroyStorageIntent {
    private final boolean isVolatile;
    private final String raftNodeId;
    private final LogStorageFactory logStorageFactory;
    private final Path serverDataPath;

    /** Constructor. */
    public DestroyStorageIntent(String raftNodeId, LogStorageFactory logStorageFactory, Path serverDataPath, boolean isVolatile) {
        this.raftNodeId = raftNodeId;
        this.logStorageFactory = logStorageFactory;
        this.serverDataPath = serverDataPath;
        this.isVolatile = isVolatile;
    }

    public String nodeId() {
        return raftNodeId;
    }

    public boolean isVolatile() {
        return isVolatile;
    }

    public LogStorageFactory logStorageFactory() {
        return logStorageFactory;
    }

    public Path serverDataPath() {
        return serverDataPath;
    }

    @Override
    public String toString() {
        return S.toString(DestroyStorageIntent.class, this);
    }
}