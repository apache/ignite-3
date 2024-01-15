package org.apache.ignite.internal.table;

import javax.cache.Cache;

public class CacheEntryImpl<K, V> implements Cache.Entry<K, V> {
    private final K key;
    private final V value;

    public CacheEntryImpl(K key, V value) {
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
