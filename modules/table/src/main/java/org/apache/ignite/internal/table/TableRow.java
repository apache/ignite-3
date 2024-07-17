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

package org.apache.ignite.internal.table;

import java.util.Objects;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.table.Tuple;

/**
 * Row to Tuple adapter.
 *
 * <p>Provides methods to access columns values by column names.
 */
public class TableRow extends MutableRowTupleAdapter {
    /**
     * Returns tuple for row key chunk.
     *
     * @param row Row.
     * @return Tuple.
     */
    public static Tuple keyTuple(Row row) {
        return new KeyRowChunk(row);
    }

    /**
     * Returns tuple for row value chunk.
     *
     * @param row Row.
     * @return Tuple.
     */
    public static Tuple valueTuple(Row row) {
        return new ValueRowChunk(row);
    }

    /**
     * Returns tuple for whole row.
     *
     * @param row Row.
     * @return Tuple.
     */
    public static Tuple tuple(Row row) {
        return new TableRow(row);
    }

    /**
     * Constructor.
     *
     * @param row Row.
     */
    private TableRow(Row row) {
        super(row);
    }

    /**
     * Key column chunk.
     */
    private static class KeyRowChunk extends MutableRowTupleAdapter {
        /**
         * Creates tuple for key chunk.
         *
         * @param row Row
         */
        KeyRowChunk(Row row) {
            super(row);
        }

        /** {@inheritDoc} */
        @Override
        public int columnCount() {
            return tuple != null ? tuple.columnCount() : schema().keyColumns().size();
        }

        /** {@inheritDoc} */
        @Override
        public int columnIndex(String columnName) {
            if (tuple != null) {
                return tuple.columnIndex(columnName);
            }

            Objects.requireNonNull(columnName);

            var col = schema().column(IgniteNameUtils.parseSimpleName(columnName));

            return col == null ? -1 : col.positionInKey();
        }

        /** {@inheritDoc} */
        @Override
        protected Column rowColumnByName(String columnName) {
            final Column col = super.rowColumnByName(columnName);

            if (col.positionInKey() == -1) {
                throw new IllegalArgumentException("Invalid column name: columnName=" + columnName);
            }

            return col;
        }

        /** {@inheritDoc} */
        @Override
        protected Column rowColumnByIndex(int columnIndex) {
            Objects.checkIndex(columnIndex, schema().keyColumns().size());

            return schema().keyColumns().get(columnIndex);
        }
    }

    /**
     * Value column chunk.
     */
    private static class ValueRowChunk extends MutableRowTupleAdapter {
        /**
         * Creates tuple for value chunk.
         *
         * @param row Row.
         */
        ValueRowChunk(Row row) {
            super(row);
        }

        /** {@inheritDoc} */
        @Override
        public int columnCount() {
            return tuple != null ? tuple.columnCount() : schema().valueColumns().size();
        }

        /** {@inheritDoc} */
        @Override
        public int columnIndex(String columnName) {
            if (tuple != null) {
                return tuple.columnIndex(columnName);
            }

            Objects.requireNonNull(columnName);

            var col = schema().column(IgniteNameUtils.parseSimpleName(columnName));

            return col == null ? -1 : col.positionInValue();
        }

        /** {@inheritDoc} */
        @Override
        protected Column rowColumnByName(String columnName) {
            final Column col = super.rowColumnByName(columnName);

            if (col.positionInKey() >= 0) {
                throw new IllegalArgumentException("Invalid column name: columnName=" + columnName);
            }

            return col;
        }

        /** {@inheritDoc} */
        @Override
        protected Column rowColumnByIndex(int columnIndex) {
            Objects.checkIndex(columnIndex, schema().valueColumns().size());

            return schema().valueColumns().get(columnIndex);
        }
    }
}
