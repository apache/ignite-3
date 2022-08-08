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

import java.nio.charset.StandardCharsets;
import org.rocksdb.RocksDB;

/**
 * Utilities for converting partition IDs and index names into Column Family names and vice versa.
 */
class ColumnFamilyUtils {
    /**
     * Name of the meta column family matches default columns family, meaning that it always exist when new table is created.
     */
    static final String META_CF_NAME = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8);

    /**
     * Name of the Column Family that stores partition data.
     */
    static final String PARTITION_CF_NAME = "cf-part";

    /**
     * Utility enum to describe a type of the column family - meta or partition.
     */
    enum ColumnFamilyType {
        META, PARTITION, UNKNOWN
    }

    /**
     * Determines column family type by its name.
     *
     * @param cfName Column family name.
     * @return Column family type.
     */
    static ColumnFamilyType columnFamilyType(String cfName) {
        if (META_CF_NAME.equals(cfName)) {
            return ColumnFamilyType.META;
        }

        if (PARTITION_CF_NAME.equals(cfName)) {
            return ColumnFamilyType.PARTITION;
        }

        return ColumnFamilyType.UNKNOWN;
    }
}
