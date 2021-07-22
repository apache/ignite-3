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

import java.util.List;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

/**
 * Wrapper for the column family that encapsulates {@link ColumnFamilyHandle} and RocksDB's operations with it.
 */
class ColumnFamily implements AutoCloseable {
    /** RocksDB instance. */
    private final RocksDB db;

    /** Column family type. */
    private final StorageColumnFamilyType cfType;

    /** Column family handle. */
    private final ColumnFamilyHandle cfHandle;

    /** Column family options. */
    private final ColumnFamilyOptions cfOptions;

    /** Options for the column family options. */
    private final Options options;

    /**
     * Constructor.
     *
     * @param db Db.
     * @param handle Column family handle.
     * @param cfType Column family type.
     * @param cfOptions Column family options.
     * @param options Options for the column family options.
     * @throws RocksDBException If failed.
     */
    ColumnFamily(
        RocksDB db,
        ColumnFamilyHandle handle,
        StorageColumnFamilyType cfType,
        ColumnFamilyOptions cfOptions,
        Options options
    ) {
        this.db = db;
        this.cfType = cfType;
        this.cfOptions = cfOptions;
        this.options = options;
        this.cfHandle = handle;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        IgniteUtils.closeAll(cfHandle, cfOptions, options);
    }

    /**
     * Gets the value associated with the key from this column family.
     *
     * @param key Key.
     * @return Value.
     * @throws RocksDBException If failed.
     * @see RocksDB#get(ColumnFamilyHandle, byte[])
     */
    byte @Nullable [] get(byte @NotNull [] key) throws RocksDBException {
        return db.get(cfHandle, key);
    }

    /**
     * Puts a key-value pair into this column family within the write batch.
     *
     * @param batch Write batch.
     * @param key Key.
     * @param value Value.
     * @throws RocksDBException If failed.
     * @see WriteBatch#put(ColumnFamilyHandle, byte[], byte[])
     */
    void put(WriteBatch batch, byte @NotNull [] key, byte @NotNull [] value) throws RocksDBException {
        batch.put(cfHandle, key, value);
    }

    /**
     * Deletes the entry mapped by the key and associated with this column family within the write batch.
     *
     * @param batch Write batch.
     * @param key Key.
     * @throws RocksDBException If failed.
     * @see WriteBatch#delete(ColumnFamilyHandle, byte[])
     */
    void delete(WriteBatch batch, byte @NotNull [] key) throws RocksDBException {
        batch.delete(cfHandle, key);
    }

    /**
     * Creates a new iterator over this column family.
     *
     * @return Iterator.
     * @see RocksDB#newIterator(ColumnFamilyHandle)
     */
    RocksIterator newIterator() {
        return db.newIterator(cfHandle);
    }

    /**
     * Creates a new iterator with given read options over this column family.
     *
     * @param options Read options.
     * @return Iterator.
     * @see RocksDB#newIterator(ColumnFamilyHandle, ReadOptions)
     */
    RocksIterator newIterator(ReadOptions options) {
        return db.newIterator(cfHandle, options);
    }

    /**
     * Ingests external files into this column family.
     *
     * @param paths Paths to the external files.
     * @param options Ingestion options.
     * @throws RocksDBException If failed.
     * @see RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)
     */
    void ingestExternalFile(List<String> paths, IngestExternalFileOptions options) throws RocksDBException {
        db.ingestExternalFile(cfHandle, paths, options);
    }

    /**
     * @return Name of the column family.
     */
    String name() {
        return cfType.name();
    }
}
