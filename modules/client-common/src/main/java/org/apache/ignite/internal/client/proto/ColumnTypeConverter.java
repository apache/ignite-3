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

package org.apache.ignite.internal.client.proto;

import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * SQL column type utils.
 */
public class ColumnTypeConverter {
    /** Enum values. */
    private static final ColumnType[] VALS = ColumnType.values();

    /**
     * Converts wire SQL type code to column type.
     *
     * @param ordinal Type code.
     * @return Column type.
     */
    public static ColumnType fromOrdinalOrThrow(int ordinal) {
        var columnType = fromOrdinal(ordinal);

        if (columnType == null) {
            throw new IgniteException(Client.PROTOCOL_ERR, "Invalid column type code: " + ordinal);
        }

        return columnType;
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable
    private static ColumnType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
