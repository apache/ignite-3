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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.rocksdb.LogFile;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;

/**
 * Calculates RocksDB size on disk. Currently, does not account for manifest files.
 */
class RocksDbSizeCalculator {
    private final RocksDB db;
    private final SstFileManager sstFileManager;

    RocksDbSizeCalculator(RocksDB db, SstFileManager sstFileManager) {
        this.db = db;
        this.sstFileManager = sstFileManager;
    }

    long totalBytesOnDisk() {
        return sstFilesTotalSize() + walFilesTotalSize();
    }

    private long sstFilesTotalSize() {
        return sstFileManager.getTotalSize();
    }

    private long walFilesTotalSize() {
        try {
            return db.getSortedWalFiles().stream()
                    .mapToLong(LogFile::sizeFileBytes)
                    .sum();
        } catch (RocksDBException e) {
            throw new IgniteInternalException(INTERNAL_ERR, e);
        }
    }
}
