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

package org.apache.ignite.internal.storage.rocksdb;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.rocksdb.RocksDBException;

/**
 * Wrapper around the "meta" Column Family inside a RocksDB-based storage, which stores some auxiliary information needed for internal
 * storage logic.
 */
class RocksDbMetaStorage implements AutoCloseable {
    /**
     * Name of the key that corresponds to a list of existing partition IDs of a storage.
     */
    private static final byte[] PARTITIONS_LIST_KEY = "partition-list".getBytes(StandardCharsets.UTF_8);

    private final ColumnFamily metaCf;

    RocksDbMetaStorage(ColumnFamily metaCf) {
        this.metaCf = metaCf;
    }

    /**
     * Returns a list of partition IDs that exist in the associated storage.
     *
     * @return list of partition IDs
     * @throws RocksDBException if RocksDB fails to read the partition list
     */
    int[] getPartitionsList() throws RocksDBException {
        byte[] partitionsList = metaCf.get(PARTITIONS_LIST_KEY);

        if (partitionsList == null) {
            return new int[0];
        }

        IntBuffer buf = ByteBuffer.wrap(partitionsList).asIntBuffer();

        int[] result = new int[buf.remaining()];

        buf.get(result);

        return result;
    }

    /**
     * Saves the given partition IDs into the meta Column Family.
     *
     * @param partitionIds array of partition IDs
     * @throws RocksDBException if RocksDB fails to write the partition list
     */
    void putPartitionsList(int[] partitionIds) throws RocksDBException {
        var buf = ByteBuffer.allocate(partitionIds.length * Integer.BYTES);

        buf.asIntBuffer().put(partitionIds);

        metaCf.put(PARTITIONS_LIST_KEY, buf.array());
    }

    @Override
    public void close() throws Exception {
        metaCf.close();
    }
}
