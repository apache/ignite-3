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

package org.apache.ignite.internal.catalog.descriptors;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Table descriptor.
 */
public class CatalogTableDescriptor extends CatalogObjectDescriptor {
    private static final long serialVersionUID = -2021394971104316570L;

    public static final int INITIAL_TABLE_VERSION = 1;

    private final int zoneId;

    private final int pkIndexId;

    private final int tableVersion;

    private final List<CatalogTableColumnDescriptor> columns;
    private final List<String> primaryKeyColumns;
    private final List<String> colocationColumns;

    @IgniteToStringExclude
    private transient Map<String, CatalogTableColumnDescriptor> columnsMap;

    /**
     * Constructor.
     *
     * @param id Table id.
     * @param pkIndexId Primary key index id.
     * @param name Table name.
     * @param zoneId Distribution zone ID.
     * @param tableVersion Version of the table.
     * @param columns Table column descriptors.
     * @param pkCols Primary key column names.
     * @param colocationCols Colocation column names.
     */
    public CatalogTableDescriptor(
            int id,
            int pkIndexId,
            String name,
            int zoneId,
            int tableVersion,
            List<CatalogTableColumnDescriptor> columns,
            List<String> pkCols,
            @Nullable List<String> colocationCols,
            long causalityToken
    ) {
        super(id, Type.TABLE, name, causalityToken);

        this.pkIndexId = pkIndexId;
        this.zoneId = zoneId;
        this.tableVersion = tableVersion;
        this.columns = Objects.requireNonNull(columns, "No columns defined.");
        primaryKeyColumns = Objects.requireNonNull(pkCols, "No primary key columns.");
        colocationColumns = colocationCols == null ? pkCols : colocationCols;

        this.columnsMap = columns.stream().collect(Collectors.toMap(CatalogTableColumnDescriptor::name, Function.identity()));

        // TODO: IGNITE-19082 Throw proper exceptions.
        assert !columnsMap.isEmpty() : "No columns.";

        assert primaryKeyColumns.stream().noneMatch(c -> Objects.requireNonNull(columnsMap.get(c), c).nullable());
        assert Set.copyOf(primaryKeyColumns).containsAll(colocationColumns);
    }

    /**
     * Returns column descriptor for column with given name.
     */
    public CatalogTableColumnDescriptor columnDescriptor(String columnName) {
        return columnsMap.get(columnName);
    }

    public int zoneId() {
        return zoneId;
    }

    public int primaryKeyIndexId() {
        return pkIndexId;
    }

    public int tableVersion() {
        return tableVersion;
    }

    public List<String> primaryKeyColumns() {
        return primaryKeyColumns;
    }

    public List<String> colocationColumns() {
        return colocationColumns;
    }

    public List<CatalogTableColumnDescriptor> columns() {
        return columns;
    }

    public CatalogTableColumnDescriptor column(String name) {
        return columnsMap.get(name);
    }

    public boolean isPrimaryKeyColumn(String name) {
        return primaryKeyColumns.contains(name);
    }

    public boolean isColocationColumn(String name) {
        return colocationColumns.contains(name);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.columnsMap = columns.stream().collect(Collectors.toMap(CatalogTableColumnDescriptor::name, Function.identity()));
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
