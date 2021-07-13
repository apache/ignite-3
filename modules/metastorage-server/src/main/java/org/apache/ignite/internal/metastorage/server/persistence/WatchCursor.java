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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.metastorage.server.EntryEvent;
import org.apache.ignite.internal.metastorage.server.Value;
import org.apache.ignite.internal.metastorage.server.WatchEvent;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 *
 */
class WatchCursor implements Cursor<WatchEvent> {
    private final RocksDBKeyValueStorage storage;
    /**
     *
     */
    private final Predicate<byte[]> p;

    /**
     *
     */
    private final Iterator<WatchEvent> it;

    /**
     * Options for {@link #nativeIterator}.
     */
    private final ReadOptions options = new ReadOptions().setPrefixSameAsStart(true);

    /**
     * RocksDB iterator.
     */
    @Nullable
    private final RocksIterator nativeIterator;

    /**
     *
     */
    private long lastRetRev;

    /**
     *
     */
    private long nextRetRev = -1;

    /**
     *
     */
    WatchCursor(RocksDBKeyValueStorage storage, long rev, Predicate<byte[]> p) {
        this.storage = storage;
        this.p = p;
        this.lastRetRev = rev - 1;
        this.nativeIterator = storage.db().newIterator(options);
        this.it = createIterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean hasNext() {
        return it.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override public WatchEvent next() {
        return it.next();
    }

    /**
     * {@inheritDoc}
     */
    @Override public void close() throws Exception {
        IgniteUtils.closeAll(Set.of(nativeIterator, options));
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override public Iterator<WatchEvent> iterator() {
        return it;
    }

    @NotNull
    Iterator<WatchEvent> createIterator() {
        return new Iterator<>() {
            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                storage.lock().readLock().lock();

                try {
                    if (nextRetRev != -1)
                        return true;

                    while (true) {
                        long curRev = lastRetRev + 1;

                        byte[] revisionPrefix = ByteUtils.longToBytes(curRev);

                        boolean empty = true;

                        if (!nativeIterator.isValid())
                            try {
                                nativeIterator.refresh();
                            }
                            catch (RocksDBException e) {
                                throw new IgniteInternalException(e);
                            }

                        for (nativeIterator.seek(revisionPrefix); nativeIterator.isValid(); nativeIterator.next()) {
                            empty = false;

                            byte[] key = RocksDBKeyValueStorage.rocksKeyToBytes(nativeIterator.key());

                            if (p.test(key)) {
                                nextRetRev = curRev;

                                return true;
                            }
                        }

                        RocksDBKeyValueStorage.checkIterator(nativeIterator);

                        if (empty)
                            return false;

                        lastRetRev++;
                    }
                }
                finally {
                    storage.lock().readLock().unlock();
                }
            }

            /** {@inheritDoc} */
            @Override public WatchEvent next() {
                storage.lock().readLock().lock();

                try {
                    while (true) {
                        if (!hasNext())
                            return null;
                        else if (nextRetRev != -1) {
                            boolean empty = true;

                            List<EntryEvent> evts = new ArrayList<>();

                            for (; nativeIterator.isValid(); nativeIterator.next()) {
                                empty = false;

                                byte[] key = RocksDBKeyValueStorage.rocksKeyToBytes(nativeIterator.key());

                                Value val = RocksDBKeyValueStorage.bytesToValue(nativeIterator.value());

                                if (p.test(key)) {
                                    Entry newEntry;

                                    if (val.tombstone())
                                        newEntry = Entry.tombstone(key, nextRetRev, val.updateCounter());
                                    else
                                        newEntry = new Entry(key, val.bytes(), nextRetRev, val.updateCounter());

                                    Entry oldEntry = storage.doGet(key, nextRetRev - 1, false);

                                    evts.add(new EntryEvent(oldEntry, newEntry));
                                }
                            }

                            RocksDBKeyValueStorage.checkIterator(nativeIterator);

                            if (empty)
                                return null;

                            if (evts.isEmpty())
                                continue;

                            lastRetRev = nextRetRev;

                            nextRetRev = -1;

                            return new WatchEvent(evts);
                        }
                    }
                }
                finally {
                    storage.lock().readLock().unlock();
                }
            }
        };
    }
}
