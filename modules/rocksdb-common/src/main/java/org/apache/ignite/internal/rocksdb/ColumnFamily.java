/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.rocksdb;

import static org.apache.ignite.internal.tracing.TracingManager.span;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

/**
 * Wrapper for the column family that encapsulates {@link ColumnFamilyHandle} and RocksDB's operations with it.
 */
public class ColumnFamily {
    /** RocksDB instance. */
    private final RocksDB db;

    /** Column family name. */
    private final String cfName;

    /** Column family name as a byte array. */
    private final byte[] cfNameBytes;

    /** Column family handle. */
    private final ColumnFamilyHandle cfHandle;

    /** Private ColumnFamilyOptions owned exclusively by this CF, if any. */
    @Nullable
    private final ColumnFamilyOptions privateCfOptions;

    /**
     * Constructor.
     *
     * @param db Db.
     * @param handle Column family handle.
     */
    private ColumnFamily(RocksDB db, ColumnFamilyHandle handle, @Nullable ColumnFamilyOptions privateCfOptions) throws RocksDBException {
        this.db = db;
        this.cfHandle = handle;
        cfNameBytes = cfHandle.getName();
        this.cfName = new String(cfNameBytes, StandardCharsets.UTF_8);
        this.privateCfOptions = privateCfOptions;
    }

    /**
     * Creates a new Column Family in the provided RocksDB instance.
     * <b>Warning!!</b> This method assumes that the ColumnFamilyOptions in the descriptor are exclusive to this ColumnFamily, as such,
     * {@link #destroy()} will close them.
     *
     * @param db RocksDB instance.
     * @param descriptor Column Family descriptor.
     * @return new Column Family.
     * @throws RocksDBException If an error has occurred during creation.
     */
    public static ColumnFamily withPrivateOptions(RocksDB db, ColumnFamilyDescriptor descriptor) throws RocksDBException {
        ColumnFamilyHandle cfHandle = db.createColumnFamily(descriptor);

        return new ColumnFamily(db, cfHandle, descriptor.getOptions());
    }

    /**
     * Creates a wrapper around an already created Column Family.
     *
     * @param db RocksDB instance.
     * @param handle Column Family handle.
     * @return Column Family wrapper.
     * @throws RocksDBException If an error has occurred during creation.
     */
    public static ColumnFamily wrap(RocksDB db, ColumnFamilyHandle handle) throws RocksDBException {
        return new ColumnFamily(db, handle, null);
    }

    /**
     * Removes all data associated with this Column Family and frees its resources.
     *
     * @throws RocksDBException if an error has occurred during the destruction.
     */
    public void destroy() throws RocksDBException {
        db.dropColumnFamily(cfHandle);

        db.destroyColumnFamilyHandle(cfHandle);

        // If we are tracking the options then we also close them.
        if (this.privateCfOptions != null) {
            privateCfOptions.close();
        }
    }

    /**
     * Gets the value associated with the key from this column family.
     *
     * @param key Key.
     * @return Value.
     * @throws RocksDBException If failed.
     * @see RocksDB#get(ColumnFamilyHandle, byte[])
     */
    public byte @Nullable [] get(byte[] key) throws RocksDBException {
        return db.get(cfHandle, key);
    }

    /**
     * Puts a key-value pair into this column family.
     *
     * @param key Key.
     * @param value Value.
     * @throws RocksDBException If failed.
     * @see RocksDB#put(ColumnFamilyHandle, byte[], byte[])
     */
    public void put(byte[] key, byte[] value) throws RocksDBException {
        try (TraceSpan ignored = span("columnFamilyPut")) {
            db.put(cfHandle, key, value);
        }
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
    public void put(WriteBatch batch, byte[] key, byte[] value) throws RocksDBException {
        batch.put(cfHandle, key, value);
    }

    /**
     * Deletes the entry mapped by the key and associated with this column family.
     *
     * @param key Key.
     * @throws RocksDBException If failed.
     * @see RocksDB#delete(ColumnFamilyHandle, byte[])
     */
    public void delete(byte[] key) throws RocksDBException {
        db.delete(cfHandle, key);
    }

    /**
     * Deletes the entry mapped by the key and associated with this column family within the write batch.
     *
     * @param batch Write batch.
     * @param key Key.
     * @throws RocksDBException If failed.
     * @see WriteBatch#delete(ColumnFamilyHandle, byte[])
     */
    public void delete(WriteBatch batch, byte[] key) throws RocksDBException {
        batch.delete(cfHandle, key);
    }

    /**
     * Removes all data between {@code start} (inclusive) and {@code end} (exclusive) keys.
     *
     * @param start start of the range (inclusive)
     * @param end end of the range (exclusive)
     * @throws RocksDBException if RocksDB fails to perform the operation
     */
    public void deleteRange(byte[] start, byte[] end) throws RocksDBException {
        db.deleteRange(cfHandle, start, end);
    }

    /**
     * Creates a new iterator over this column family.
     *
     * @return Iterator.
     * @see RocksDB#newIterator(ColumnFamilyHandle)
     */
    public RocksIterator newIterator() {
        return db.newIterator(cfHandle);
    }

    /**
     * Creates a new iterator with given read options over this column family.
     *
     * @param options Read options.
     * @return Iterator.
     * @see RocksDB#newIterator(ColumnFamilyHandle, ReadOptions)
     */
    public RocksIterator newIterator(ReadOptions options) {
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
    public void ingestExternalFile(List<String> paths, IngestExternalFileOptions options) throws RocksDBException {
        db.ingestExternalFile(cfHandle, paths, options);
    }

    /**
     * Returns column family handle.
     *
     * @return Column family handle.
     */
    public ColumnFamilyHandle handle() {
        return cfHandle;
    }

    /**
     * Returns the private column family options, if any.
     *
     * @return The ColumnFamilyOptions, if they are exclusive to this column family.
     */
    @Nullable
    public ColumnFamilyOptions privateOptions() {
        return privateCfOptions;
    }

    /**
     * Returns name of the column family.
     *
     * @return Name of the column family.
     */
    public String name() {
        return cfName;
    }

    /**
     * Returns the name of the column family, represented as a byte array.
     */
    public byte[] nameBytes() {
        return cfNameBytes;
    }

    /**
     * Returns the RocksDB instance that contains this Column Family.
     *
     * @return RocksDB instance that contains this Column Family.
     */
    public RocksDB db() {
        return db;
    }
}
