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

import static org.apache.ignite.internal.catalog.CatalogManager.INITIAL_TIMESTAMP;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.S;

/** Sorted index descriptor. */
public class CatalogSortedIndexDescriptor extends CatalogIndexDescriptor {
    private final List<CatalogIndexColumnDescriptor> columns;

    /**
     * Constructs a sorted index descriptor in status {@link CatalogIndexStatus#REGISTERED}.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param columns A list of columns descriptors.
     *
     * @throws IllegalArgumentException If columns list contains duplicates or columns size doesn't match the collations size.
     */
    public CatalogSortedIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            List<CatalogIndexColumnDescriptor> columns
    ) {
        this(id, name, tableId, unique, REGISTERED, columns, false);
    }

    /**
     * Constructs a sorted index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param status Index status.
     * @param columns A list of columns descriptors.
     * @param isCreatedWithTable Flag indicating that this index has been created at the same time as its table.
     *
     * @throws IllegalArgumentException If columns list contains duplicates or columns size doesn't match the collations size.
     */
    public CatalogSortedIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            List<CatalogIndexColumnDescriptor> columns,
            boolean isCreatedWithTable
    ) {
        this(id, name, tableId, unique, status, columns, INITIAL_TIMESTAMP, isCreatedWithTable);
    }

    /**
     * Constructs a sorted index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param status Index status.
     * @param columns A list of columns descriptors.
     * @param timestamp Timestamp of the update of the descriptor.
     * @param isCreatedWithTable Flag indicating that this index has been created at the same time as its table.
     *
     * @throws IllegalArgumentException If columns list contains duplicates or columns size doesn't match the collations size.
     */
    CatalogSortedIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            List<CatalogIndexColumnDescriptor> columns,
            HybridTimestamp timestamp,
            boolean isCreatedWithTable
    ) {
        super(CatalogIndexDescriptorType.SORTED, id, name, tableId, unique, status, timestamp, isCreatedWithTable);

        this.columns = Objects.requireNonNull(columns, "columns");
    }

    /** Returns indexed columns. */
    public List<CatalogIndexColumnDescriptor> columns() {
        return columns;
    }

    @Override
    @SuppressWarnings("removal")
    public CatalogSortedIndexDescriptor upgradeIfNeeded(CatalogTableDescriptor table) {
        if (columns.isEmpty() || columns.get(0).name() == null) {
            return this;
        }

        assert tableId() == table.id();

        List<CatalogIndexColumnDescriptor> upgradedColumns = new ArrayList<>(columns.size());
        for (CatalogIndexColumnDescriptor indexColumn : columns) {
            String columnName = indexColumn.name();

            assert columnName != null;

            CatalogTableColumnDescriptor column = table.column(columnName);

            assert column != null : columnName;

            upgradedColumns.add(
                    new CatalogIndexColumnDescriptor(
                            column.id(),
                            indexColumn.collation()
                    )
            );
        }

        return new CatalogSortedIndexDescriptor(
                id(), name(), tableId(), unique(), status(), upgradedColumns, updateTimestamp(), isCreatedWithTable()
        );
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DESCRIPTOR_SORTED_INDEX.id();
    }

    @Override
    public String toString() {
        return S.toString(CatalogSortedIndexDescriptor.class, this, super.toString());
    }
}
