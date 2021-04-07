package org.apache.ignite.raft.jraft.storage.impl;

import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;

public class RocksDBLogManagerTest extends LogManagerTest {
    @Override protected LogStorage newLogStorage(RaftOptions raftOptions) {
        return new RocksDBLogStorage(this.path, new RaftOptions());
    }
}
