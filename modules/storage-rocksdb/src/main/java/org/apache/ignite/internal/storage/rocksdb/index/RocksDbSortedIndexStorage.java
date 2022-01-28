/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.rocksdb.index;

import java.util.function.Predicate;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * {@link SortedIndexStorage} implementation based on RocksDB.
 */
public class RocksDbSortedIndexStorage implements SortedIndexStorage {
    private final ColumnFamily indexCf;

    private final SortedIndexDescriptor descriptor;

    private final IndexRowDeserializer indexRowDeserializer;

    private final IndexRowSerializer indexRowSerializer;

    /**
     * Creates a new Index storage.
     *
     * @param indexCf Column Family for storing the data.
     * @param descriptor Index descriptor.
     */
    public RocksDbSortedIndexStorage(ColumnFamily indexCf, SortedIndexDescriptor descriptor) {
        this.indexCf = indexCf;
        this.descriptor = descriptor;

        BinaryIndexRowSerializer serializer = new BinaryIndexRowSerializer(descriptor);
        this.indexRowSerializer = serializer;
        this.indexRowDeserializer = serializer;
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public void put(IndexRow row) {
        assert row.primaryKey().bytes().length > 0;

        try {
            IndexBinaryRow binRow = indexRowSerializer.serialize(row);

            indexCf.put(binRow.keySlice(), binRow.valueSlice());
        } catch (RocksDBException e) {
            throw new StorageException("Error while adding data to Rocks DB", e);
        }
    }

    @Override
    public void remove(IndexRow row) {
        try {
            IndexBinaryRow binRow = indexRowSerializer.serialize(row);

            indexCf.delete(binRow.keySlice());
        } catch (RocksDBException e) {
            throw new StorageException("Error while removing data from Rocks DB", e);
        }
    }

    @Override
    public Cursor<IndexRow> range(IndexRowPrefix low, IndexRowPrefix up, Predicate<IndexRow> filter) {
        RocksIterator iter = indexCf.newIterator();

        iter.seekToFirst();

        return new RocksIteratorAdapter<>(iter) {
            @Nullable
            private PrefixComparator lowerBoundComparator = low != null ? new PrefixComparator(descriptor, low) : null;

            private final PrefixComparator upperBoundComparator = up != null ? new PrefixComparator(descriptor, up) : null;

            @Override
            public boolean hasNext() {
                while (super.hasNext()) {
                    var row = new ByteBufferRow(it.key());

                    if (lowerBoundComparator != null) {
                        // if lower comparator is not null, then the lower bound has not yet been reached
                        if (lowerBoundComparator.compare(row) < 0) {
                            it.next();

                            continue;
                        } else {
                            // once the lower bound is reached, we no longer need to check it
                            lowerBoundComparator = null;
                        }
                    }

                    return upperBoundComparator == null || upperBoundComparator.compare(row) <= 0;
                }

                return false;
            }

            @Override
            protected IndexRow decodeEntry(byte[] key, byte[] value) {
                return indexRowDeserializer.deserialize(new IndexBinaryRowImpl(key, value));
            }
        };
    }

    @Override
    public void close() throws Exception {
        indexCf.close();
    }

    @Override
    public void destroy() {
        try {
            indexCf.destroy();
        } catch (Exception e) {
            throw new StorageException(String.format("Failed to destroy index \"%s\"", descriptor.name()), e);
        }
    }
}
