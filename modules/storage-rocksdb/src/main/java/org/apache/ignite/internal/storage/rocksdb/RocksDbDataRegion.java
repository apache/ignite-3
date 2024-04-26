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

package org.apache.ignite.internal.storage.rocksdb;

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileView;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.WriteBufferManager;

/**
 * Data region implementation for {@link RocksDbStorageEngine}. Based on a {@link Cache}.
 */
public class RocksDbDataRegion {
    /** Region configuration view. */
    private final RocksDbProfileView storageProfileView;

    /** RocksDB cache instance. */
    private Cache cache;

    /** Write buffer manager instance. */
    private WriteBufferManager writeBufferManager;

    /**
     * Constructor.
     *
     * @param storageProfileView Storage profile configuration view.
     */
    public RocksDbDataRegion(RocksDbProfileView storageProfileView) {
        this.storageProfileView = storageProfileView;
    }

    /**
     * Start the rocksDb data region.
     */
    public void start() {
        long writeBufferSize = storageProfileView.writeBufferSize();

        long totalCacheSize = storageProfileView.size() + writeBufferSize;

        cache = new LRUCache(totalCacheSize, storageProfileView.numShardBits(), false);

        writeBufferManager = new WriteBufferManager(writeBufferSize, cache);
    }

    /**
     * Starts the rocksDb data region.
     */
    public void stop() throws Exception {
        closeAll(writeBufferManager, cache);
    }

    /**
     * Returns write buffer manager associated with the region.
     */
    public WriteBufferManager writeBufferManager() {
        return writeBufferManager;
    }
}
