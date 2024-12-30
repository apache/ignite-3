package org.apache.ignite.raft.jraft.storage.impl;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.raft.jraft.storage.DestroyStorageIntentStorage;

public class NoopDestroyStorageIntentStorage implements DestroyStorageIntentStorage {
    @Override
    public Collection<String> storagesToDestroy(String factoryName) {
        return Collections.emptyList();
    }

    @Override
    public void saveDestroyIntent(String factoryName, String uri) {
    }

    @Override
    public void removeDestroyIntent(String factoryName, String uri) {
    }
}
