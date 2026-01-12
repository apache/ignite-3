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

package org.apache.ignite.internal.sql;

import static org.apache.ignite.lang.util.IgniteNameUtils.quoteIfNeeded;

import java.util.List;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Metadata of the column of a query result set.
 */
public class ColumnMetadataImpl implements ColumnMetadata {
    /**
     * Creates column origin from list.
     *
     * @param origin Origin list.
     * @return Column origin.
     */
    public static @Nullable ColumnOrigin originFromList(@Nullable List<String> origin) {
        if (origin == null) {
            return null;
        }

        String schemaName = origin.isEmpty() ? "" : origin.get(0);
        String tableName = origin.size() < 2 ? "" : origin.get(1);
        String columnName = origin.size() < 3 ? "" : origin.get(2);

        return new ColumnOriginImpl(schemaName, tableName, columnName);
    }

    /** Name of the result's column. */
    private final String name;

    /** Type of the result's column. */
    private final ColumnType type;

    /** Column precision. */
    private final int precision;

    /** Column scale. */
    private final int scale;

    /** Nullable flag of the result's column. */
    private final boolean nullable;

    /** Origin of the result's column. */
    private final ColumnOrigin origin;

    /**
     * Constructor.
     */
    public ColumnMetadataImpl(
            String name,
            ColumnType type,
            int precision,
            int scale,
            boolean nullable,
            @Nullable ColumnOrigin origin
    ) {
        this.name = name;
        this.type = type;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
        this.origin = origin;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name.isEmpty() ? name : quoteIfNeeded(name);
    }

    /** {@inheritDoc} */
    @Override
    public ColumnType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override
    public int precision() {
        return precision;
    }

    /** {@inheritDoc} */
    @Override
    public int scale() {
        return scale;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> valueClass() {
        return type.javaClass();
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnOrigin origin() {
        return origin;
    }

    /** Returns normalized name. */
    String normalizedName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ColumnMetadataImpl.class,  this);
    }

    /**
     * Metadata of column's origin.
     */
    public static class ColumnOriginImpl implements ColumnOrigin {
        /** Schema name. */
        private final String schemaName;

        /** Table name. */
        private final String tableName;

        /** Column name. */
        private final String columnName;

        /**
         * Constructor.
         */
        public ColumnOriginImpl(String schemaName, String tableName, String columnName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.columnName = columnName;
        }

        /** {@inheritDoc} */
        @Override
        public String schemaName() {
            return quoteIfNeeded(schemaName);
        }

        /** {@inheritDoc} */
        @Override
        public String tableName() {
            return quoteIfNeeded(tableName);
        }

        /** {@inheritDoc} */
        @Override
        public String columnName() {
            return columnName != null ? quoteIfNeeded(columnName) : null;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(ColumnOriginImpl.class,  this);
        }
    }
}
