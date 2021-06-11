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
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.storage.api.DataRow;
import org.apache.ignite.storage.api.InvokeClosure;
import org.apache.ignite.storage.api.SearchRow;
import org.apache.ignite.storage.api.SimpleDataRow;
import org.apache.ignite.storage.api.Storage;
import org.apache.ignite.storage.api.StorageException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class RocksDbStorage implements Storage, AutoCloseable {
    private final RocksDB db;

    public RocksDbStorage(Path dbPath, Comparator<ByteBuffer> comparator) throws IgniteInternalCheckedException {
        try {
            Options options = new Options();

            options.setComparator(new AbstractComparator(new ComparatorOptions()) {
                @Override public String name() {
                    return null;
                }

                @Override public int compare(ByteBuffer a, ByteBuffer b) {
                    return comparator.compare(a, b);
                }
            });

            this.db = RocksDB.open(options, dbPath.toAbsolutePath().toString());
        }
        catch (RocksDBException e) {
            throw new IgniteInternalCheckedException("adfsaf", e);
        }
    }

    @NotNull private static byte[] getBytes(ByteBuffer keyBuf) {
        keyBuf.rewind();

        byte[] keyBytes = new byte[keyBuf.remaining()];
        keyBuf.get(keyBytes);
        return keyBytes;
    }

    private static ByteBuffer wrap(byte[] existingDataBytes) {
        return ByteBuffer.wrap(existingDataBytes).asReadOnlyBuffer();
    }

    @Override public DataRow read(SearchRow key) throws StorageException {
        ByteBuffer keyBuf = key.key();

        try {
            byte[] keyBytes = getBytes(keyBuf);

            return new SimpleDataRow(key.key(), wrap(db.get(keyBytes)));
        }
        catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    @Override public void write(DataRow row) throws StorageException {
        try {
            db.put(getBytes(row.key()), getBytes(row.value()));
        }
        catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    @Override public void remove(SearchRow key) throws StorageException {
        try {
            db.delete(getBytes(key.key()));
        }
        catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    @Override public void invoke(DataRow key, InvokeClosure clo) throws StorageException {
        synchronized (this) {
            try {
                byte[] keyBytes = getBytes(key.key());
                byte[] existingDataBytes = db.get(keyBytes);

                clo.call(new SimpleDataRow(key.key(), wrap(existingDataBytes)));

                switch (clo.operationType()) {
                    case PUT:
                        db.put(keyBytes, getBytes(clo.newRow().value()));

                        break;

                    case REMOVE:
                        db.delete(keyBytes);

                        break;

                    case NOOP:
                        break;
                }
            }
            catch (RocksDBException e) {
                throw new StorageException(e);
            }
        }
    }

    @Override public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {
        return new ScanCursor(db.newIterator(), filter);
    }

    @Override public void close() throws Exception {
        db.close();
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

        @NotNull @Override public Iterator<DataRow> iterator() {
            throw new UnsupportedOperationException("iterator");
        }

        @Override public boolean hasNext() {
            while (iter.isValid() && !filter.test(new SimpleDataRow(wrap(iter.key()), null)))
                iter.next();

            return iter.isValid();
        }

        @Override public DataRow next() {
            if (!hasNext())
                throw new NoSuchElementException();

            var row = new SimpleDataRow(wrap(iter.key()), wrap(iter.value()));

            iter.next();

            return row;
        }

        @Override public void close() throws Exception {
            iter.close();
        }
    }
}
