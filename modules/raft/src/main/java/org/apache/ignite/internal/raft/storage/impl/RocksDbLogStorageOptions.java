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

package org.apache.ignite.internal.raft.storage.impl;

import org.apache.ignite.internal.configuration.SystemLocalView;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.rocksdb.util.SizeUnit;

/**
 * RocksDB-specific options for RocksDB-based log storage.
 */
public class RocksDbLogStorageOptions {
    private static final IgniteLogger LOG = Loggers.forClass(RocksDbLogStorageOptions.class);

    private static final String PARTITIONS_RAFT_LOG_STORAGE_WRITE_BUFFER_SIZE_PROPERTY_NAME = "partitionsRaftLogStorageWriteBufferSize";

    private static final long DEFAULT_WRITE_BUFFER_SIZE = 64 * SizeUnit.MB;

    private final long writeBufferSize;

    public static RocksDbLogStorageOptions forPartitions(SystemLocalView properties) {
        return new RocksDbLogStorageOptions(partitionsRaftLogStorageWriteBufferSize(properties));
    }

    private static long partitionsRaftLogStorageWriteBufferSize(SystemLocalView properties) {
        SystemPropertyView property = properties.properties().get(PARTITIONS_RAFT_LOG_STORAGE_WRITE_BUFFER_SIZE_PROPERTY_NAME);

        if (property == null) {
            return DEFAULT_WRITE_BUFFER_SIZE;
        }

        try {
            return Long.parseLong(property.propertyValue());
        } catch (NumberFormatException e) {
            LOG.warn(
                    "Failed to parse partitions writeBufferSize '{}', default value will be used ({})",
                    e,
                    property.propertyValue(),
                    DEFAULT_WRITE_BUFFER_SIZE
            );

            return DEFAULT_WRITE_BUFFER_SIZE;
        }
    }

    public static RocksDbLogStorageOptions defaults() {
        return new RocksDbLogStorageOptions(DEFAULT_WRITE_BUFFER_SIZE);
    }

    private RocksDbLogStorageOptions(long writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
    }

    public long writeBufferSize() {
        return writeBufferSize;
    }
}
