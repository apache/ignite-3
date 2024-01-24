package org.apache.ignite.internal.client;

import javax.cache.Cache;

public class ClientCacheEntryImpl<K, V> implements Cache.Entry<K, V> {
    private final K key;
    private final V value;

    public ClientCacheEntryImpl(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }
}

