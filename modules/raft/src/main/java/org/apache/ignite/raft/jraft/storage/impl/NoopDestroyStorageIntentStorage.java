package org.apache.ignite.raft.jraft.storage.impl;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.raft.jraft.storage.DestroyStorageIntentStorage;

public class NoopDestroyStorageIntentStorage implements DestroyStorageIntentStorage {
    @Override
    public Collection<String> storagesToDestroy(String storagePrefix) {
        return Collections.emptyList();
    }

    @Override
    public void saveDestroyStorageIntent(String storagePrefix, String uri) {
    }

    @Override
    public void removeDestroyStorageIntent(String storagePrefix, String uri) {
    }
}
