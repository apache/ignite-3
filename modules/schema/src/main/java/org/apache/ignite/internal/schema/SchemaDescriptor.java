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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.internal.marshaller.MarshallerColumn;
import org.apache.ignite.internal.marshaller.MarshallerSchema;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.util.HashCalculator;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Describes schema of main components of the table.
 *
 * <p>Contains layout for full row tuple, key tuple, as well as enumeration of columns representing
 * colocation key in the order the should be supplied to {@link HashCalculator} in order to
 * compute colocation hash.
 */
public class SchemaDescriptor {
    /** Schema version. Incremented on each schema modification. */
    private final int ver;

    private final List<Column> columns;

    /** Key columns in serialization order. */
    private final List<Column> keyCols;

    /** Value columns in serialization order. */
    private final List<Column> valCols;

    /** Colocation columns. */
    private final List<Column> colocationCols;

    /** Mapping 'Column name' -&gt; Column. */
    private final Map<String, Column> columnsByName;

    /** Whether schema contains time or timestamp columns. */
    private final boolean hasTemporalColumns;

    /** Column mapper. */
    private ColumnMapper colMapper = ColumnMapping.identityMapping();

    /** Marshaller schema. */
    private MarshallerSchema marshallerSchema;

    /**
     * Constructor.
     *
     * @param ver Schema version.
     * @param keyCols Key columns.
     * @param valCols Value columns.
     */
    @TestOnly
    public SchemaDescriptor(int ver, Column[] keyCols, Column[] valCols) {
        this(
                ver,
                mergeColumns(keyCols, valCols),
                Arrays.stream(keyCols).map(Column::name).collect(Collectors.toList()),
                null
        );
    }

    /** Constructor. */
    public SchemaDescriptor(
            int ver,
            List<Column> columns,
            List<String> keyColumns,
            @Nullable List<String> colocationColumns
    ) {
        assert !nullOrEmpty(columns) : "Schema should have at least one column";

        Map<String, Column> columnsByName = new HashMap<>();
        List<Column> orderedColumns = new ArrayList<>(columns.size());

        Object2IntMap<String> columnNameToPositionInKey = toElementToPositionMap(keyColumns);

        Object2IntMap<String> columnNameToPositionInColocation;
        if (colocationColumns == null) {
            columnNameToPositionInColocation = columnNameToPositionInKey;
        } else {
            columnNameToPositionInColocation = toElementToPositionMap(colocationColumns);

            assert columnNameToPositionInKey.keySet().containsAll(colocationColumns)
                    : "Colocation column must be part of the key: keyCols="
                    + keyColumns + ", colocationCols=" + colocationColumns;
        }

        boolean hasTemporalColumns = false;
        int rowPosition = 0;
        int valuePosition = 0;
        for (Column column : columns) {
            Column orderedColumn = column.copy(
                    rowPosition++,
                    columnNameToPositionInKey.getOrDefault(column.name(), -1),
                    columnNameToPositionInKey.containsKey(column.name()) ? -1 : valuePosition++,
                    columnNameToPositionInColocation.getOrDefault(column.name(), -1)
            );

            Column old = columnsByName.put(orderedColumn.name(), orderedColumn);

            assert old == null : "Columns with similar names are not allowed: " + old.name();

            orderedColumns.add(orderedColumn);

            if (column.type() instanceof TemporalNativeType) {
                hasTemporalColumns = true;
            }
        }

        this.ver = ver;
        this.columns = List.copyOf(orderedColumns);
        this.columnsByName = Map.copyOf(columnsByName);
        this.hasTemporalColumns = hasTemporalColumns;

        List<Column> tmpKeyColumns = new ArrayList<>(keyColumns.size());

        BitSet keyColumnsBitSet = new BitSet(columns.size());
        for (String name : keyColumns) {
            Column column = columnsByName.get(name);

            assert column != null : name;
            assert !column.nullable() : "Primary key cannot contain nullable column: " + name;

            tmpKeyColumns.add(column);

            assert !keyColumnsBitSet.get(column.positionInRow()) : column.name();

            keyColumnsBitSet.set(column.positionInRow());
        }

        this.keyCols = List.copyOf(tmpKeyColumns);

        this.colocationCols = colocationColumns == null
                ? this.keyCols
                : colocationColumns.stream()
                        .map(columnsByName::get)
                        .collect(Collectors.toList());

        List<Column> tmpValueColumns = new ArrayList<>(columns.size() - keyColumnsBitSet.cardinality());
        for (Column column : orderedColumns) {
            if (!keyColumnsBitSet.get(column.positionInRow())) {
                tmpValueColumns.add(column);
            }
        }

        this.valCols = List.copyOf(tmpValueColumns);
    }

    private static List<Column> mergeColumns(Column[] keyColumns, Column[] valueColumns) {
        List<Column> columns = new ArrayList<>(keyColumns.length + valueColumns.length);

        Collections.addAll(columns, keyColumns);
        Collections.addAll(columns, valueColumns);

        return columns;
    }

    /**
     * Get schema version.
     *
     * @return Schema version.
     */
    public int version() {
        return ver;
    }

    /**
     * Get column by index.
     *
     * @param colIdx Column index.
     * @return Column instance.
     */
    public Column column(int colIdx) {
        validateColumnIndex(colIdx);

        return columns.get(colIdx);
    }

    /**
     * Get column by name.
     *
     * @param name Column name.
     * @return Column.
     */
    public @Nullable Column column(String name) {
        return columnsByName.get(name);
    }

    /** Returns columns in the order their appear in serialized tuple. */
    public List<Column> columns() {
        return columns;
    }

    /**
     * Validates the column index.
     *
     * @param colIdx Column index.
     */
    private void validateColumnIndex(int colIdx) {
        Objects.checkIndex(colIdx, length());
    }

    /**
     * Get key columns.
     *
     * @return Key columns chunk.
     */
    public List<Column> keyColumns() {
        return keyCols;
    }

    /**
     * Get colocation columns.
     *
     * @return Key colocation columns chunk.
     */
    public List<Column> colocationColumns() {
        return colocationCols;
    }

    /**
     * Get value columns.
     *
     * @return Value columns chunk.
     */
    public List<Column> valueColumns() {
        return valCols;
    }

    /**
     * Get total number of columns in schema.
     *
     * @return Total number of columns in schema.
     */
    public int length() {
        return columns.size();
    }

    /**
     * Sets column mapper for previous schema version.
     *
     * @param colMapper Column mapper.
     */
    public void columnMapping(ColumnMapper colMapper) {
        this.colMapper = colMapper;
    }

    /**
     * Get column mapper.
     *
     * @return Column mapper.
     */
    public ColumnMapper columnMapping() {
        return colMapper;
    }

    /**
     * Get a value indicating whether schema contains temporal columns.
     *
     * @return {@code true} if schema contains temporal columns (time, datetime, timestamp), {@code false} otherwise.
     */
    public boolean hasTemporalColumns() {
        return hasTemporalColumns;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(SchemaDescriptor.class, this);
    }

    /** Returns marshaller schema. */
    public MarshallerSchema marshallerSchema() {
        if (marshallerSchema == null) {
            marshallerSchema = new ServerMarshallerSchema(this);
        }
        return marshallerSchema;
    }

    private static class ServerMarshallerSchema implements MarshallerSchema {

        private final SchemaDescriptor schema;

        private MarshallerColumn[] keys;

        private MarshallerColumn[] values;

        private MarshallerColumn[] row;

        private ServerMarshallerSchema(SchemaDescriptor schema) {
            this.schema = schema;
        }

        @Override
        public int schemaVersion() {
            return schema.version();
        }

        @Override
        public MarshallerColumn[] keys() {
            if (keys == null) {
                keys = MarshallerUtil.toMarshallerColumns(schema.keyColumns());
            }
            return keys;
        }

        @Override
        public MarshallerColumn[] values() {
            if (values == null) {
                values = MarshallerUtil.toMarshallerColumns(schema.valueColumns());
            }
            return values;
        }

        @Override
        public MarshallerColumn[] row() {
            if (row == null) {
                row = MarshallerUtil.toMarshallerColumns(schema.columns());
            }
            return row;
        }
    }

    private static Object2IntMap<String> toElementToPositionMap(List<String> elements) {
        Object2IntMap<String> result = new Object2IntOpenHashMap<>();
        int idx = 0;
        for (String element : elements) {
            assert !result.containsKey(element)
                    : "Elements should not have duplicates: " + element;

            result.put(element, idx++);
        }

        return result;
    }
}
