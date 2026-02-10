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

import static org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema.UNSPECIFIED_SIZE;

import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileConfiguration;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileView;
import org.apache.ignite.internal.util.IgniteUtils;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.WriteBufferManager;

/**
 * Storage profile implementation for {@link RocksDbStorageEngine}. Based on a {@link Cache}.
 */
public class RocksDbStorageProfile {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RocksDbStorageProfile.class);

    /** Profile configuration view. */
    private final RocksDbProfileConfiguration storageProfileConfig;

    /** RocksDB cache instance. */
    private Cache cache;

    /** Write buffer manager instance. */
    private WriteBufferManager writeBufferManager;

    private volatile long regionSize;

    /**
     * Constructor.
     *
     * @param storageProfileConfig Storage profile configuration view.
     */
    public RocksDbStorageProfile(RocksDbProfileConfiguration storageProfileConfig) {
        this.storageProfileConfig = storageProfileConfig;
    }

    /**
     * Start the profile.
     */
    public void start() {
        long writeBufferSize = storageProfileConfig.writeBufferSizeBytes().value();

        regionSize = sizeBytes();

        long totalCacheSize = sizeBytes() + writeBufferSize;

        cache = new LRUCache(totalCacheSize, -1, false);

        writeBufferManager = new WriteBufferManager(writeBufferSize, cache);
    }

    private long sizeBytes() {
        var storageProfileConfigView = (RocksDbProfileView) storageProfileConfig.value();
        long dataRegionSize = storageProfileConfigView.sizeBytes();

        if (dataRegionSize == UNSPECIFIED_SIZE) {
            dataRegionSize = StorageEngine.defaultDataRegionSize();

            LOG.info(
                    "{}.{} property is not specified, setting its value to {}",
                    storageProfileConfigView.name(), storageProfileConfig.writeBufferSizeBytes().key(), dataRegionSize
            );
        }

        return dataRegionSize;
    }

    long regionSize() {
        return regionSize;
    }

    /**
     * Returns profile name.
     */
    public String name() {
        return storageProfileConfig.value().name();
    }

    /**
     * Closes and frees resources associated with this profile.
     */
    public void stop() throws Exception {
        IgniteUtils.closeAll(writeBufferManager, cache);
    }

    /**
     * Returns write buffer manager associated with the profile.
     */
    public WriteBufferManager writeBufferManager() {
        return writeBufferManager;
    }
}
