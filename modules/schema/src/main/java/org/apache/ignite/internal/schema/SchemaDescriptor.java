/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.internal.schema;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.NotNull;
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

    /** Mapping 'Column name' -&gt; Column. */
    private final Map<String, Column> colMap;

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
    public SchemaDescriptor(int ver, Column[] keyCols, @Nullable String[] colocationCols, Column[] valCols) {
        assert keyCols.length > 0 : "No key columns are configured.";

        this.ver = ver;
        this.keyCols = new Columns(0, keyCols);
        this.valCols = new Columns(keyCols.length, valCols);

        assert this.keyCols.nullMapSize() == 0 : "Primary key cannot contain nullable column [cols=" + this.keyCols + ']';

        colMap = new LinkedHashMap<>(keyCols.length + valCols.length);

        Stream.concat(Arrays.stream(this.keyCols.columns()), Arrays.stream(this.valCols.columns()))
                .sorted(Comparator.comparingInt(Column::columnOrder))
                .forEach(c -> colMap.put(c.name(), c));

        // Preserving key chunk column order is not actually required.
        // It is sufficient to has same column order for all nodes.
        this.colocationCols = (ArrayUtils.nullOrEmpty(colocationCols)) ? this.keyCols.columns() :
                Arrays.stream(colocationCols).map(colMap::get).toArray(Column[]::new);
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
    public @Nullable Column column(@NotNull String name) {
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

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(SchemaDescriptor.class, this);
    }
}
