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

import static org.apache.ignite.internal.util.IgniteUtils.newHashMap;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.marshaller.MarshallerColumn;
import org.apache.ignite.internal.marshaller.MarshallerSchema;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Full schema descriptor containing key columns chunk, value columns chunk, and schema version.
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
    private final List<Column> fullRowColocationCols;

    private final List<Column> keyOnlyColocationCols;

    /** Colocation columns. */
    private final @Nullable Map<Column, Integer> colocationColIndexes;

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
     * @param ver     Schema version.
     * @param keyCols Key columns.
     * @param valCols Value columns.
     */
    public SchemaDescriptor(int ver, Column[] keyCols, Column[] valCols) {
        this(ver, keyCols, null, valCols);
    }

    /** Constructor. */
    public SchemaDescriptor(int ver, Column[] keyCols, String @Nullable[] colocationCols, Column[] valCols) {
        this(
                ver,
                Stream.concat(Arrays.stream(keyCols), Arrays.stream(valCols)).collect(Collectors.toList()),
                Arrays.stream(keyCols).map(Column::name).collect(Collectors.toList()),
                colocationCols != null
                        ? Arrays.stream(keyCols).map(Column::name).collect(Collectors.toList())
                        : null
        );
    }

    /** Constructor. */
    public SchemaDescriptor(int ver, List<Column> columns, List<String> keyColumns, @Nullable List<String> colocationColumns) {
        assert !keyColumns.isEmpty() : "No key columns are configured.";

        this.ver = ver;
        Map<String, Column> columnsByName = new HashMap<>();
        List<Column> orderedColumns = new ArrayList<>(columns.size());

        boolean hasTemporalColumns = false;
        int order = 0;
        for (Column column : columns) {
            Column orderedColumn = column.copy(order++);

            columnsByName.put(orderedColumn.name(), orderedColumn);
            orderedColumns.add(orderedColumn);

            if (column.type() instanceof TemporalNativeType) {
                hasTemporalColumns = true;
            }
        }

        this.columns = List.copyOf(orderedColumns);
        this.columnsByName = Map.copyOf(columnsByName);
        this.hasTemporalColumns = hasTemporalColumns;

        List<Column> tmpKeyColumns = new ArrayList<>(keyColumns.size());

        BitSet keyColumnsBitSet = new BitSet(columns.size());
        Object2IntMap<String> keyColumnToPositionInKey = new Object2IntOpenHashMap<>(keyColumns.size());
        int idx = 0;
        for (String name : keyColumns) {
            keyColumnToPositionInKey.put(name, idx++);

            Column column = columnsByName.get(name);

            assert column != null : name;

            tmpKeyColumns.add(column);
            keyColumnsBitSet.set(column.order());
        }

        this.keyCols = List.copyOf(tmpKeyColumns);

        List<Column> tmpValueColumns = new ArrayList<>(columns.size() - keyColumnsBitSet.cardinality());
        for (Column column : orderedColumns) {
            if (!keyColumnsBitSet.get(column.order())) {
                tmpValueColumns.add(column);
            }
        }

        this.valCols = List.copyOf(tmpValueColumns);

        if (!CollectionUtils.nullOrEmpty(colocationColumns)) {
            this.fullRowColocationCols = colocationColumns.stream()
                    .map(columnsByName::get)
                    .collect(Collectors.toList());

            this.colocationColIndexes = newHashMap(fullRowColocationCols.size());
            for (int i = 0; i < fullRowColocationCols.size(); i++) {
                colocationColIndexes.put(fullRowColocationCols.get(i), i);
            }
        } else {
            this.fullRowColocationCols = this.keyCols;
            this.colocationColIndexes = null;
        }

        List<Column> tmp = new ArrayList<>(fullRowColocationCols.size());

        for (Column col : fullRowColocationCols) {
            tmp.add(col.copy(keyColumnToPositionInKey.getInt(col.name())));
        }

        this.keyOnlyColocationCols = List.copyOf(tmp);
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
     * Check if column index belongs to the key column.
     *
     * @param idx Column index to check.
     * @return {@code true} if the column belongs to the key chunk, {@code false} otherwise.
     */
    public boolean isKeyColumn(int idx) {
        validateColumnIndex(idx);

        return idx < keyCols.size();
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
    public void validateColumnIndex(int colIdx) {
        Objects.checkIndex(colIdx, length());
    }

    /**
     * Gets columns names.
     *
     * @return Columns names.
     */
    public Collection<String> columnNames() {
        return columns.stream().map(Column::name).collect(Collectors.toList());
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
    public List<Column> fullRowColocationColumns() {
        return fullRowColocationCols;
    }

    public List<Column> keyOnlyColocationColumns() {
        return keyOnlyColocationCols;
    }

    /**
     * Get colocation index of the specified column.
     *
     * @param col Column.
     * @return Index in the colocationColumns array, or -1 when not applicable.
     */
    public int colocationIndex(Column col) {
        return colocationColIndexes == null
                ? -1
                : colocationColIndexes.getOrDefault(col, -1);
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
}
