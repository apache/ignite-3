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

package org.apache.ignite.internal.jdbc;

import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/** Column definition. */
public final class ColumnDefinition {
    public final String label;
    public final String schema;
    public final String table;
    public final String column;
    public final ColumnType type;
    public final int precision;
    public final int scale;
    public final boolean nullable;

    public ColumnDefinition(
            String label,
            ColumnType type,
            int precision,
            int scale,
            boolean nullable
    ) {
        this(label, null, null, null, type, precision, scale, nullable);
    }

    private ColumnDefinition(
            String label,
            @Nullable String schema,
            @Nullable String table,
            @Nullable String column,
            ColumnType type,
            int precision,
            int scale,
            boolean nullable
    ) {
        this.label = label;
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.type = type;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    ColumnDefinition withOrigin(@Nullable String schema, @Nullable String table, @Nullable String column) {
        return new ColumnDefinition(label, schema, table, column, type, precision, scale, nullable);
    }
}
