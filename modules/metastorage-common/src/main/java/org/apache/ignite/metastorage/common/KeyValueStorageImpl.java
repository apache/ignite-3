package org.apache.ignite.metastorage.common;

import org.apache.ignite.internal.metastorage.common.DummyEntry;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

// TODO: IGNITE-14389 tmp, should be removed.
public class KeyValueStorageImpl implements KeyValueStorage {

    @Override public long revision() {
        return 0;
    }

    @Override public long updateCounter() {
        return 0;
    }

    @Override public @NotNull Entry get(byte[] key) {
        return null;
    }

    @Override public @NotNull Entry get(byte[] key, long rev) {
        return null;
    }

    @Override public @NotNull List<Entry> getAll(List<byte[]> keys) {
        return null;
    }

    @Override public @NotNull List<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        return null;
    }

    @Override public void put(byte[] key, byte[] value) {

    }

    @Override public @NotNull Entry getAndPut(byte[] key, byte[] value) {
        return null;
    }

    @Override public void putAll(List<byte[]> keys, List<byte[]> values) {

    }

    @Override public @NotNull List<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values) {
        return null;
    }

    @Override public void remove(byte[] key) {

    }

    @Override public @NotNull Entry getAndRemove(byte[] key) {
        return null;
    }

    @Override public void removeAll(List<byte[]> keys) {

    }

    @Override public @NotNull List<Entry> getAndRemoveAll(List<byte[]> keys) {
        return null;
    }

    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo) {
        return null;
    }

    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
        return null;
    }

    @Override public Cursor<WatchEvent> watch(byte[] keyFrom, byte[] keyTo, long rev) {
        return new Cursor<WatchEvent>() {
            @Override public void close() throws Exception {

            }

            @NotNull @Override public Iterator<WatchEvent> iterator() {
                return new Iterator<WatchEvent>() {
                    @Override public boolean hasNext() {
                        return true;
                    }

                    @Override public WatchEvent next() {
                        return new WatchEvent(
                            new DummyEntry(
                                new Key(new byte[]{1}),
                                new byte[]{2},
                                1L,
                                1L
                            ),
                            new DummyEntry(
                                new Key(new byte[]{1}),
                                new byte[]{3},
                                2L,
                                2L
                            )
                        );
                    }
                };
            }
        };
    }

    @Override public Cursor<WatchEvent> watch(byte[] key, long rev) {
        return null;
    }

    @Override public Cursor<WatchEvent> watch(Collection<byte[]> keys, long rev) {
        return null;
    }

    @Override public void compact() {

    }
}
