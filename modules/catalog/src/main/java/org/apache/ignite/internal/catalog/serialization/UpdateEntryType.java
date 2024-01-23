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

package org.apache.ignite.internal.catalog.serialization;

import org.jetbrains.annotations.Nullable;

/**
 * Update entry type.
 */
public enum UpdateEntryType {
    ALTER_COLUMN(0),
    ALTER_ZONE(1),
    NEW_ZONE(2),
    DROP_COLUMN(3),
    DROP_INDEX(4),
    DROP_TABLE(5),
    DROP_ZONE(6),
    MAKE_INDEX_AVAILABLE(7),
    START_BUILDING_INDEX(8),
    NEW_COLUMN(9),
    NEW_INDEX(10),
    NEW_SYS_VIEW(11),
    NEW_TABLE(12),
    RENAME_TABLE(13),
    ID_GENERATOR(14);

    private final int id;

    private static final UpdateEntryType[] VALS = new UpdateEntryType[values().length];

    static {
        for (UpdateEntryType entryType : values()) {
            assert VALS[entryType.id] == null : "Found duplicate id " + entryType.id;

            VALS[entryType.id()] = entryType;
        }
    }

    UpdateEntryType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static @Nullable UpdateEntryType getById(int id) {
        return id >= 0 && id < VALS.length ? VALS[id] : null;
    }
}
