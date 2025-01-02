package org.apache.ignite.raft.jraft.storage.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.raft.jraft.util.BytesUtil.EMPTY_BYTES;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.raft.jraft.storage.DestroyStorageIntentStorage;

/** Uses VaultManager to persist and retrieve log storage destroy intents. */
public class VaultDestroyStorageIntentStorage implements DestroyStorageIntentStorage {
    private static final ByteArray LOG_STORAGES_PREFIX = new ByteArray("destroy.logStorages.");
    private static final ByteOrder BYTE_UTILS_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    private final VaultManager vault;

    /** Constructor. */
    public VaultDestroyStorageIntentStorage(VaultManager vault) {
        this.vault = vault;
    }

    @Override
    public Collection<String> storagesToDestroy(String storagePrefix) {
        ByteArray prefix = prefixByFactoryName(storagePrefix);

        try (Cursor<VaultEntry> cursor = vault.prefix(prefix)) {
            List<String> result = new ArrayList<>();

            while (cursor.hasNext()) {
                result.add(uriFromKey(storagePrefix, cursor.next().key().bytes()));
            }

            return result;
        }
    }

    @Override
    public void saveDestroyStorageIntent(String storagePrefix, String uri) {
        vault.put(buildKey(storagePrefix, uri), EMPTY_BYTES);
    }

    @Override
    public void removeDestroyStorageIntent(String storagePrefix, String uri) {
        vault.remove(buildKey(storagePrefix, uri));
    }

    private static ByteArray buildKey(String storagePrefix, String uri) {
        byte[] key = ByteBuffer.allocate(LOG_STORAGES_PREFIX.length() + storagePrefix.length() + uri.length())
                .order(BYTE_UTILS_BYTE_ORDER)
                .put(LOG_STORAGES_PREFIX.bytes())
                .put(storagePrefix.getBytes(UTF_8))
                .putChar('.')
                .put(uri.getBytes(UTF_8))
                .array();

        return new ByteArray(key);
    }

    private static String uriFromKey(String factoryName, byte[] key) {
        int offset = LOG_STORAGES_PREFIX.length() + factoryName.length() + 1;

        return new String(key, offset, key.length - offset, UTF_8);
    }

    private static ByteArray prefixByFactoryName(String factoryName) {
        byte[] buffer = ByteBuffer.allocate(LOG_STORAGES_PREFIX.length() + factoryName.length())
                .order(BYTE_UTILS_BYTE_ORDER)
                .put(LOG_STORAGES_PREFIX.bytes())
                .put(factoryName.getBytes(UTF_8))
                .array();

        return new ByteArray(buffer);
    }
}
