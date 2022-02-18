/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metastorage.server.persistence;

import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.bytesToValue;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.longToBytes;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.revisionFromRocksKey;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.rocksKeyToBytes;
import static org.apache.ignite.internal.rocksdb.RocksUtils.checkIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.metastorage.server.EntryEvent;
import org.apache.ignite.internal.metastorage.server.Value;
import org.apache.ignite.internal.metastorage.server.WatchEvent;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Subscription on updates of entries corresponding to the given keys range (where the upper bound is unlimited) and starting from the given
 * revision number.
 */
class WatchCursor implements Cursor<WatchEvent> {
    /** Storage. */
    private final RocksDbKeyValueStorage storage;

    /** Key predicate. */
    private final Predicate<byte[]> predicate;

    /** Iterator for this cursor. */
    private final Iterator<WatchEvent> it;

    /** Options for {@link #nativeIterator}. */
    private final ReadOptions options = new ReadOptions();

    /** RocksDB iterator. */
    private final RocksIterator nativeIterator;

    /** Current revision. */
    private long currentRevision;

    /** Current value of the inner iterator's hasNext that is being reset to {@code false} after next is called. */
    private boolean currentHasNext = false;

    /**
     * Constructor.
     *
     * @param storage Storage.
     * @param rev     Starting revision.
     * @param predicate       Key predicate.
     */
    WatchCursor(RocksDbKeyValueStorage storage, long rev, Predicate<byte[]> predicate) {
        this.storage = storage;
        this.predicate = predicate;

        this.currentRevision = rev;

        this.nativeIterator = storage.newDataIterator(options);
        this.nativeIterator.seek(longToBytes(rev));

        this.it = createIterator();
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public WatchEvent next() {
        return it.next();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        IgniteUtils.closeAll(options, nativeIterator);
    }

    /** {@inheritDoc} */
    @NotNull
    @Override
    public Iterator<WatchEvent> iterator() {
        return it;
    }

    /**
     * Creates an iterator for this cursor.
     *
     * @return Iterator.
     */
    @NotNull
    private Iterator<WatchEvent> createIterator() {
        return new Iterator<>() {
            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                storage.lock().readLock().lock();

                try {
                    if (currentHasNext) {
                        return true;
                    }

                    if (!nativeIterator.isValid()) {
                        try {
                            nativeIterator.refresh();

                            nativeIterator.seek(longToBytes(currentRevision));
                        } catch (RocksDBException e) {
                            throw new IgniteInternalException(e);
                        }
                    }

                    // Check all keys to see if any one of them match the predicate.
                    for (; nativeIterator.isValid(); nativeIterator.next()) {
                        byte[] rocksKey = nativeIterator.key();

                        byte[] key = rocksKeyToBytes(rocksKey);

                        if (predicate.test(key)) {
                            checkIterator(nativeIterator);

                            // We may have jumped to the next revision if there were no matching keys in previous.
                            currentRevision = revisionFromRocksKey(rocksKey);

                            currentHasNext = true;

                            return true;
                        }
                    }

                    checkIterator(nativeIterator);

                    return false;
                } finally {
                    storage.lock().readLock().unlock();
                }
            }

            /** {@inheritDoc} */
            @Nullable
            @Override
            public WatchEvent next() {
                storage.lock().readLock().lock();

                try {
                    if (!hasNext()) {
                        return null;
                    }

                    List<EntryEvent> evts = new ArrayList<>();

                    // Iterate over the keys of the current revision and get all matching entries.
                    for (; nativeIterator.isValid(); nativeIterator.next()) {
                        byte[] rocksKey = nativeIterator.key();
                        byte[] rocksValue = nativeIterator.value();

                        long revision = revisionFromRocksKey(rocksKey);

                        if (revision > currentRevision) {
                            // There are no more keys for the current revision
                            break;
                        }

                        byte[] key = rocksKeyToBytes(rocksKey);

                        Value val = bytesToValue(rocksValue);

                        if (predicate.test(key)) {
                            Entry newEntry;

                            if (val.tombstone()) {
                                newEntry = Entry.tombstone(key, revision, val.updateCounter());
                            } else {
                                newEntry = new Entry(key, val.bytes(), revision, val.updateCounter());
                            }

                            Entry oldEntry = storage.doGet(key, revision - 1, false);

                            evts.add(new EntryEvent(oldEntry, newEntry));
                        }
                    }

                    currentHasNext = false;

                    // Go to the next revision
                    currentRevision++;

                    checkIterator(nativeIterator);

                    return new WatchEvent(evts);
                } finally {
                    storage.lock().readLock().unlock();
                }
            }
        };
    }
}
