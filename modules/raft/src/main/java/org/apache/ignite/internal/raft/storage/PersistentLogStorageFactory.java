package org.apache.ignite.internal.raft.storage;

import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.raft.jraft.storage.DestroyStorageIntentStorage;

/** Persistent log storage factory. */
// TODO https://issues.apache.org/jira/browse/IGNITE-22766.
public interface PersistentLogStorageFactory extends LogStorageFactory, LogSyncer {
    /** Destroys "leftover" storages saved to {@link DestroyStorageIntentStorage}. Must be called on factory start. */
    void completeLogStoragesDestruction();
}
