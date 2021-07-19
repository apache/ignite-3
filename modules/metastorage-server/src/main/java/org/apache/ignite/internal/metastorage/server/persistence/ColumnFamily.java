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
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

public class ColumnFamily implements AutoCloseable {

    private final RocksDB db;

    private final StorageColumnFamilyType family;

    private final ColumnFamilyHandle columnFamilyHandle;

    private final ColumnFamilyOptions cfOptions;

    private final Options options;

    public ColumnFamily(RocksDB db, StorageColumnFamilyType family, ColumnFamilyOptions cfOptions, Options options)
        throws RocksDBException {
        this.db = db;
        this.family = family;
        this.cfOptions = cfOptions;
        this.options = options;

        this.columnFamilyHandle = db.createColumnFamily(new ColumnFamilyDescriptor(family.nameAsBytes(), cfOptions));
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        IgniteUtils.closeAll(columnFamilyHandle, cfOptions, options);
    }

    public byte @Nullable [] get(byte @NotNull [] key) throws RocksDBException {
        return db.get(columnFamilyHandle, key);
    }

    public void put(WriteBatch batch, byte @NotNull [] key, byte @NotNull [] value) throws RocksDBException {
        batch.put(columnFamilyHandle, key, value);
    }

    public void delete(WriteBatch batch, byte @NotNull [] key) throws RocksDBException {
        batch.delete(columnFamilyHandle, key);
    }

    public ColumnFamilyHandle handle() {
        return columnFamilyHandle;
    }

    public RocksIterator newIterator() {
        return db.newIterator(columnFamilyHandle);
    }

    public RocksIterator newIterator(ReadOptions options) {
        return db.newIterator(columnFamilyHandle, options);
    }

    public void ingestExternalFile(List<String> paths, IngestExternalFileOptions options) throws RocksDBException {
        db.ingestExternalFile(columnFamilyHandle, paths, options);
    }

    public String name() {
        return family.name();
    }
}
