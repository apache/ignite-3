package org.apache.ignite.raft.jraft.storage;

import java.util.Collection;

/** Persists and retrieves intent to complete storages destruction on node start. */
public interface DestroyStorageIntentStorage extends Storage {
    /** Returns storages to destroy by given prefix (i.e. log storage factory name). */
    Collection<String> storagesToDestroy(String storagePrefix);

    /** Saves intent to destroy storage. */
    void saveDestroyStorageIntent(String storagePrefix, String uri);

    /** Removes intent to destroy storage. */
    void removeDestroyStorageIntent(String storagePrefix, String uri);
}
