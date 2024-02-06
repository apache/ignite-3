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

import static org.apache.ignite.internal.util.IgniteUtils.newLinkedHashMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Full schema descriptor containing key columns chunk, value columns chunk, and schema version.
 */
public class SchemaDescriptor {
    /** Schema version. Incremented on each schema modification. */
    private final int ver;

    /** Key columns in serialization order. */
    private final Columns keyCols;

    /** Value columns in serialization order. */
    private final Columns valCols;

    /** Colocation columns. */
    private final Column[] colocationCols;

    /** Colocation columns. */
    private final @Nullable Map<Column, Integer> colocationColIndexes;

    /** Mapping 'Column name' -&gt; Column. */
    private final Map<String, Column> colMap;

    /** Whether schema contains time or timestamp columns. */
    private final boolean hasTemporalColumns;

    private final boolean physicalOrderMatchesLogical;

    /** Column mapper. */
    private ColumnMapper colMapper = ColumnMapping.identityMapping();

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

    /**
     * Constructor.
     *
     * @param ver     Schema version.
     * @param keyCols Key columns.
     * @param colocationCols Colocation column names.
     * @param valCols Value columns.
     */
    public SchemaDescriptor(int ver, Column[] keyCols, String @Nullable[] colocationCols, Column[] valCols) {
        assert keyCols.length > 0 : "No key columns are configured.";

        this.ver = ver;
        this.keyCols = new Columns(0, keyCols);
        this.valCols = new Columns(keyCols.length, valCols);

        assert this.keyCols.nullMapSize() == 0 : "Primary key cannot contain nullable column [cols=" + this.keyCols + ']';

        colMap = newLinkedHashMap(keyCols.length + valCols.length);
        var hasTemporalColumns = new AtomicBoolean(false);

        Stream.concat(Arrays.stream(this.keyCols.columns()), Arrays.stream(this.valCols.columns()))
                .sorted(Comparator.comparingInt(Column::columnOrder))
                .forEach(c -> {
                    if (c.type() instanceof TemporalNativeType) {
                        hasTemporalColumns.set(true);
                    }

                    colMap.put(c.name(), c);
                });

        this.physicalOrderMatchesLogical = colMap.values().stream().allMatch(col -> col.columnOrder() == col.schemaIndex());

        this.hasTemporalColumns = hasTemporalColumns.get();

        // Preserving key chunk column order is not actually required.
        // It is sufficient to has same column order for all nodes.
        if (ArrayUtils.nullOrEmpty(colocationCols)) {
            this.colocationCols = this.keyCols.columns();
            this.colocationColIndexes = null;
        } else {
            this.colocationCols = new Column[colocationCols.length];
            this.colocationColIndexes = new HashMap<>(colocationCols.length);

            for (int i = 0; i < colocationCols.length; i++) {
                Column col = colMap.get(colocationCols[i]);
                this.colocationCols[i] = col;
                this.colocationColIndexes.put(col, i);
            }
        }
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

        return idx < keyCols.length();
    }

    /**
     * Get column by index.
     *
     * @param colIdx Column index.
     * @return Column instance.
     */
    public Column column(int colIdx) {
        validateColumnIndex(colIdx);

        return colIdx < keyCols.length() ? keyCols.column(colIdx) : valCols.column(colIdx - keyCols.length());
    }

    /**
     * Get column by name.
     *
     * @param name Column name.
     * @return Column.
     */
    public @Nullable Column column(String name) {
        return colMap.get(name);
    }

    /**
     * Validates the column index.
     *
     * @param colIdx Column index.
     */
    public void validateColumnIndex(int colIdx) {
        Objects.checkIndex(colIdx, length());
    }

    /** Returns true if physical order matches the logical order, false otherwise. */
    public boolean physicalOrderMatchesLogical() {
        return physicalOrderMatchesLogical;
    }

    /**
     * Gets columns names.
     *
     * @return Columns names.
     */
    public Collection<String> columnNames() {
        return colMap.keySet();
    }

    /**
     * Get key columns.
     *
     * @return Key columns chunk.
     */
    public Columns keyColumns() {
        return keyCols;
    }

    /**
     * Get colocation columns.
     *
     * @return Key colocation columns chunk.
     */
    public Column[] colocationColumns() {
        return colocationCols;
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
    public Columns valueColumns() {
        return valCols;
    }

    /**
     * Get total number of columns in schema.
     *
     * @return Total number of columns in schema.
     */
    public int length() {
        return keyCols.length() + valCols.length();
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
}
