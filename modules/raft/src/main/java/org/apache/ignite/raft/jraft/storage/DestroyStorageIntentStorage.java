package org.apache.ignite.raft.jraft.storage;

import java.util.Collection;

/** Persists and retrieves intent to complete log storages destruction on node start. */
public interface DestroyStorageIntentStorage extends Storage {
    Collection<String> storagesToDestroy(String factoryName);

    void saveDestroyIntent(String factoryName, String uri);

    void removeDestroyIntent(String factoryName, String uri);
}
