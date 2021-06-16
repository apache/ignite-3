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

package org.apache.ignite.storage.rocksdb;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.storage.api.DataRow;
import org.apache.ignite.storage.api.InvokeClosure;
import org.apache.ignite.storage.api.SearchRow;
import org.apache.ignite.storage.api.Storage;
import org.apache.ignite.storage.api.StorageException;
import org.apache.ignite.storage.api.basic.SimpleDataRow;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Storage implementation based on a single RocksDB instance.
 */
public class RocksDbStorage implements Storage, AutoCloseable {
    static {
        RocksDB.loadLibrary();
    }

    /** RocksDB comparator options. */
    private final ComparatorOptions comparatorOptions;

    /** RocksDB comparator. */
    private final AbstractComparator comparator;

    /** RockDB options. */
    private final Options options;

    /** RocksDb instance. */
    private final RocksDB db;

    /**
     * @param dbPath Path to the folder to store data.
     * @param comparator Keys comparator.
     * @throws StorageException If failed to create RocksDB instance.
     */
    public RocksDbStorage(Path dbPath, Comparator<ByteBuffer> comparator) throws StorageException {
        try {
            comparatorOptions = new ComparatorOptions();

            this.comparator = new AbstractComparator(comparatorOptions) {
                /** {@inheritDoc} */
                @Override public String name() {
                    return "comparator";
                }

                /** {@inheritDoc} */
                @Override public int compare(ByteBuffer a, ByteBuffer b) {
                    return comparator.compare(a, b);
                }
            };

            options = new Options();

            options.setCreateIfMissing(true);

            options.setComparator(this.comparator);

            this.db = RocksDB.open(options, dbPath.toAbsolutePath().toString());
        }
        catch (RocksDBException e) {
            try (this) {
                throw new StorageException("Failed to start storage", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public DataRow read(SearchRow key) throws StorageException {
        try {
            byte[] keyBytes = key.keyBytes();

            return new SimpleDataRow(keyBytes, db.get(keyBytes));
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to read data from the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void write(DataRow row) throws StorageException {
        try {
            db.put(row.keyBytes(), row.valueBytes());
        }
        catch (RocksDBException e) {
            throw new StorageException("Filed to write data to the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void remove(SearchRow key) throws StorageException {
        try {
            db.delete(key.keyBytes());
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to remove data from the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void invoke(SearchRow key, InvokeClosure clo) throws StorageException {
        try {
            byte[] keyBytes = key.keyBytes();
            byte[] existingDataBytes = db.get(keyBytes);

            clo.call(new SimpleDataRow(keyBytes, existingDataBytes));

            switch (clo.operationType()) {
                case WRITE:
                    db.put(keyBytes, clo.newRow().valueBytes());

                    break;

                case REMOVE:
                    db.delete(keyBytes);

                    break;

                case NOOP:
                    break;
            }
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to access data in the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {
        return new ScanCursor(db.newIterator(), filter);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (db != null)
            db.close();

        if (options != null)
            options.close();

        if (comparator != null)
            comparator.close();

        if (comparatorOptions != null)
            comparatorOptions.close();
    }

    private static class ScanCursor implements Cursor<DataRow> {
        private final RocksIterator iter;

        private final Predicate<SearchRow> filter;

        private ScanCursor(RocksIterator iter, Predicate<SearchRow> filter) {
            this.iter = iter;
            this.filter = filter;

            iter.seekToFirst();

            hasNext();
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<DataRow> iterator() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            while (iter.isValid() && !filter.test(new SimpleDataRow(iter.key(), null)))
                iter.next();

            return iter.isValid();
        }

        /** {@inheritDoc} */
        @Override public DataRow next() {
            if (!hasNext())
                throw new NoSuchElementException();

            var row = new SimpleDataRow(iter.key(), iter.value());

            iter.next();

            return row;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            iter.close();
        }
    }
}
