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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageFactory.FINISHED_META_MIGRATION_META_KEY;
import static org.apache.ignite.internal.raft.storage.impl.RocksDbSharedLogStorageUtils.raftNodeStorageEndPrefix;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.ArrayUtils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Implements migration of log storage metadata. Namely, it finds all log storages for which at least one key (in either Configuration
 * or default column families) exists, and marks them as 'created', so support the invariant that for a log storage on which init()
 * is called the corresponding 'created' key is in RocksDB.
 */
class MetadataMigration {
    private static final IgniteLogger LOG = Loggers.forClass(MetadataMigration.class);

    private final RocksDB db;

    private final WriteOptions writeOptions;

    private final ColumnFamilyHandle metaHandle;
    private final ColumnFamilyHandle confHandle;
    private final ColumnFamilyHandle dataHandle;

    /** Constructor. */
    MetadataMigration(
            RocksDB db,
            WriteOptions writeOptions,
            ColumnFamilyHandle metaHandle,
            ColumnFamilyHandle confHandle,
            ColumnFamilyHandle dataHandle
    ) {
        this.db = db;
        this.writeOptions = writeOptions;
        this.metaHandle = metaHandle;
        this.confHandle = confHandle;
        this.dataHandle = dataHandle;
    }

    void migrateIfNeeded() throws RocksDBException {
        if (metaMigrationIsFinished()) {
            return;
        }

        try (WriteBatch writeBatch = new WriteBatch()) {
            boolean migratingSomething = doMigration(writeBatch);

            markMetaMigrationAsFinished(writeBatch);

            db.write(writeOptions, writeBatch);

            if (migratingSomething) {
                LOG.info("Metadata migration was performed and touched some log storages.");
            }
        }
    }

    private boolean metaMigrationIsFinished() throws RocksDBException {
        return db.get(metaHandle, FINISHED_META_MIGRATION_META_KEY) != null;
    }

    private boolean doMigration(WriteBatch writeBatch) throws RocksDBException {
        boolean migratingSomething = false;

        for (String groupIdForStorage : raftNodeStorageIdsOnDisk()) {
            RocksDbSharedLogStorage.saveStorageStartedFlag(metaHandle, groupIdForStorage, writeBatch);

            migratingSomething = true;
        }

        return migratingSomething;
    }

    Set<String> raftNodeStorageIdsOnDisk() throws RocksDBException {
        Set<String> groupIdsForStorage = new HashSet<>();

        try (
                RocksIterator confIt = db.newIterator(confHandle);
                RocksIterator dataIt = db.newIterator(dataHandle)
        ) {
            confIt.seekToFirst();
            dataIt.seekToFirst();

            // Iterate both configuration and data column families in parallel to find all groupIds.
            // When we find a new groupId, we put it to the set and then we seek to next groupId to skip
            // all keys that belong to the one we just found.
            // For some groups, it is possible that only configuration CF or only data CF contain data for that
            // group, so we handle this situation appropriately.
            while (confIt.isValid() || dataIt.isValid()) {
                if (confIt.isValid() && dataIt.isValid()) {
                    byte[] confKey = confIt.key();
                    byte[] dataKey = dataIt.key();

                    int confToDataComparison = Arrays.compare(confKey, dataKey);
                    if (confToDataComparison <= 0) {
                        String idForStorage = handleGroupIdIteratorEntry(confIt, confKey, groupIdsForStorage);

                        if (confToDataComparison == 0) {
                            skipToNextGroupKey(dataIt, idForStorage);
                        }
                    } else {
                        handleGroupIdIteratorEntry(dataIt, dataKey, groupIdsForStorage);
                    }
                } else {
                    // Just one is valid.
                    RocksIterator it = confIt.isValid() ? confIt : dataIt;
                    assert it.isValid();

                    handleGroupIdIteratorEntry(it, it.key(), groupIdsForStorage);
                }
            }

            // Doing this to make an exception thrown if the iteration was stopped due to an error and not due to exhausting
            // the iteration space.
            confIt.status();
            dataIt.status();
        }

        return Set.copyOf(groupIdsForStorage);
    }

    private static String handleGroupIdIteratorEntry(RocksIterator it, byte[] currentKey, Set<String> groupIdsForStorage) {
        int indexOfZero = indexOf((byte) 0, currentKey);
        assert indexOfZero >= 0 : new String(currentKey, UTF_8) + " does not have a zero byte";

        String idForStorage = new String(currentKey, 0, indexOfZero, UTF_8);
        groupIdsForStorage.add(idForStorage);

        skipToNextGroupKey(it, idForStorage);

        return idForStorage;
    }

    private static int indexOf(byte needle, byte[] haystack) {
        for (int i = 0; i < haystack.length; i++) {
            if (haystack[i] == needle) {
                return i;
            }
        }

        return -1;
    }

    private static void skipToNextGroupKey(RocksIterator it, String idForStorage) {
        it.seek(raftNodeStorageEndPrefix(idForStorage));
    }

    private void markMetaMigrationAsFinished(WriteBatch writeBatch) throws RocksDBException {
        writeBatch.put(metaHandle, FINISHED_META_MIGRATION_META_KEY, ArrayUtils.BYTE_EMPTY_ARRAY);
    }
}
