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
import java.nio.ByteOrder;
import java.nio.file.Path;
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
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

public class RocksDbStorage implements Storage, AutoCloseable {
    private final RocksDB db;

    public RocksDbStorage(Path dbPath) throws IgniteInternalCheckedException {
        try {
            this.db = RocksDB.open(dbPath.toAbsolutePath().toString());
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
                    case IN_PLACE:
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

    @Override public Cursor<ByteBuffer> snapshot() throws StorageException {
        return new SnapshotCursor(db);
    }

    @Override public void restoreSnapshot(Cursor<ByteBuffer> snapshot) throws StorageException {
        while (snapshot.hasNext()) {
            ByteBuffer buf = snapshot.next();

            buf.order(ByteOrder.LITTLE_ENDIAN);

            int keyLength = buf.getInt();

            try {
                // Hm...
                db.put(buf.array(), 4, keyLength, buf.array(), 4 + keyLength, buf.capacity() - 4 - keyLength);
            }
            catch (RocksDBException e) {
                throw new StorageException(e);
            }
        }
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

    private static class SnapshotCursor implements Cursor<ByteBuffer> {
        private final RocksDB db;
        private final Snapshot snapshot;
        private final RocksIterator iter;

        private SnapshotCursor(RocksDB db) {
            this.db = db;
            snapshot = db.getSnapshot();
            iter = db.newIterator(new ReadOptions().setSnapshot(snapshot));

            iter.seekToFirst();

            hasNext();
        }

        @NotNull @Override public Iterator<ByteBuffer> iterator() {
            throw new UnsupportedOperationException("iterator");
        }

        @Override public boolean hasNext() {
            return iter.isValid();
        }

        @Override public ByteBuffer next() {
            if (!hasNext())
                throw new NoSuchElementException();

            byte[] keyBytes = iter.key();
            byte[] dataBytes = iter.value();

            ByteBuffer buf = ByteBuffer.allocate(4 + keyBytes.length + dataBytes.length);
            buf.order(ByteOrder.LITTLE_ENDIAN);

            buf.putInt(keyBytes.length).put(keyBytes).put(dataBytes);

            return buf;
        }

        @Override public void close() throws Exception {
            db.releaseSnapshot(snapshot);
        }
    }
}
