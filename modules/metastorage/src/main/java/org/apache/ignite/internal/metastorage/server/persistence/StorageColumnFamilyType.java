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

package org.apache.ignite.internal.metastorage.server.persistence;

import java.nio.charset.StandardCharsets;
import org.rocksdb.RocksDB;

/**
 * A type of the column family.
 */
enum StorageColumnFamilyType {
    /** Column family for the data. */
    DATA(RocksDB.DEFAULT_COLUMN_FAMILY),

    /** Column family for the index. Index is a mapping from entry key to a list of revisions of the storage. */
    INDEX("INDEX".getBytes(StandardCharsets.UTF_8)),

    /** Column family for the timestamp to revision mapping. */
    TS_TO_REVISION("TSTOREV".getBytes(StandardCharsets.UTF_8)),

    /** Column family for the revision to timestamp mapping. */
    REVISION_TO_TS("REVTOTS".getBytes(StandardCharsets.UTF_8));

    /** Byte representation of the column family's name. */
    private final byte[] nameAsBytes;

    /**
     * Constructor.
     *
     * @param bytes Column family name's bytes.
     */
    StorageColumnFamilyType(byte[] bytes) {
        nameAsBytes = bytes;
    }

    /**
     * Gets column family name's bytes.
     *
     * @return Column family name's bytes.
     */
    public byte[] nameAsBytes() {
        return nameAsBytes;
    }
}
