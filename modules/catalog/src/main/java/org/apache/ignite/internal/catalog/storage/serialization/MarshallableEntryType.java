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

package org.apache.ignite.internal.catalog.storage.serialization;

/**
 * Update log entry serialization type.
 */
public enum MarshallableEntryType {
    ALTER_COLUMN(0),
    ALTER_ZONE(1),
    NEW_ZONE(2),
    DROP_COLUMN(3),
    DROP_INDEX(4),
    DROP_TABLE(5),
    DROP_ZONE(6),
    MAKE_INDEX_AVAILABLE(7),
    REMOVE_INDEX(8),
    START_BUILDING_INDEX(9),
    NEW_COLUMN(10),
    NEW_INDEX(11),
    NEW_SYS_VIEW(12),
    NEW_TABLE(13),
    RENAME_TABLE(14),
    ID_GENERATOR(15),
    SNAPSHOT(16),
    VERSIONED_UPDATE(17),
    RENAME_INDEX(18),
    SET_DEFAULT_ZONE(19);

    /** Type ID. */
    private final int id;

    private static final MarshallableEntryType[] VALS = new MarshallableEntryType[values().length];

    static {
        for (MarshallableEntryType entryType : values()) {
            assert VALS[entryType.id] == null : "Found duplicate id " + entryType.id;

            VALS[entryType.id()] = entryType;
        }
    }

    MarshallableEntryType(int id) {
        this.id = id;
    }

    /** Returns type ID. */
    public int id() {
        return id;
    }
}
