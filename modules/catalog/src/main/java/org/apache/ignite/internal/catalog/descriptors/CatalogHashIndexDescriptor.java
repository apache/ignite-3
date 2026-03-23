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
import static org.apache.ignite.internal.util.CollectionUtils.copyOrNull;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Hash index descriptor. */
public class CatalogHashIndexDescriptor extends CatalogIndexDescriptor {
    private final @Nullable IntList columnIds;
    private final @Nullable List<String> columnNames;

    /**
     * Constructs a hash index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param status Index status.
     * @param columnNames A list of indexed columns. Must not contain duplicates.
     * @param isCreatedWithTable Flag indicating that this index has been created at the same time as its table.
     *
     * @throws IllegalArgumentException If columns list contains duplicates.
     * @deprecated This constructor is used in old deserializers. Use
     *         {@link #CatalogHashIndexDescriptor(int, String, int, boolean, CatalogIndexStatus, List, IntList, HybridTimestamp, boolean)}
     *         instead.
     */
    @Deprecated(forRemoval = true)
    public CatalogHashIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            List<String> columnNames,
            boolean isCreatedWithTable
    ) {
        this(id, name, tableId, unique, status, columnNames, null, INITIAL_TIMESTAMP, isCreatedWithTable);
    }

    /**
     * Constructs a hash index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param status Index status.
     * @param columnIds A list of indexed columns. Must not contain duplicates.
     * @param isCreatedWithTable Flag indicating that this index has been created at the same time as its table.
     *
     * @throws IllegalArgumentException If columns list contains duplicates.
     */
    public CatalogHashIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            IntList columnIds,
            boolean isCreatedWithTable
    ) {
        this(id, name, tableId, unique, status, null, columnIds, INITIAL_TIMESTAMP, isCreatedWithTable);
    }

    /**
     * Constructs a hash index descriptor.
     *
     * @param id Id of the index.
     * @param name Name of the index.
     * @param tableId Id of the table index belongs to.
     * @param unique Unique flag.
     * @param status Index status.
     * @param columnNames A list of indexed columns. Must not contain duplicates.
     * @param timestamp Timestamp of the update of the descriptor.
     * @param isCreatedWithTable Flag indicating that this index has been created at the same time as its table.
     * @throws IllegalArgumentException If columns list contains duplicates.
     */
    CatalogHashIndexDescriptor(
            int id,
            String name,
            int tableId,
            boolean unique,
            CatalogIndexStatus status,
            @Nullable List<String> columnNames,
            @Nullable IntList columnIds,
            HybridTimestamp timestamp,
            boolean isCreatedWithTable
    ) {
        super(CatalogIndexDescriptorType.HASH, id, name, tableId, unique, status, timestamp, isCreatedWithTable);

        this.columnIds = columnIds;
        this.columnNames = copyOrNull(columnNames);
    }

    /**
     * Returns names of the indexed columns.
     *
     * @return Names of the indexed columns.
     * @deprecated Non-null may be returned only during catalog recovery after cluster upgrade.
     *      Use {@link #columnIds()} instead.
     */
    @Deprecated(forRemoval = true) // still used in old serializers
    public @Nullable List<String> columns() {
        return columnNames;
    }

    @Override
    public CatalogHashIndexDescriptor upgradeIfNeeded(CatalogTableDescriptor table) {
        if (columnIds != null) {
            return this;
        }

        assert tableId() == table.id();
        assert columnNames != null;

        int[] columnIds = new int[columnNames.size()];
        for (int i = 0; i < columnIds.length; i++) {
            CatalogTableColumnDescriptor column = table.column(columnNames.get(i));

            assert column != null : columnNames.get(i);

            columnIds[i] = column.id();
        }

        return new CatalogHashIndexDescriptor(
                id(), name(), tableId(), unique(), status(), null, IntList.of(columnIds), updateTimestamp(), isCreatedWithTable()
        );
    }

    /** Returns IDs of the indexed columns. */
    public IntList columnIds() {
        return Objects.requireNonNull(columnIds);
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DESCRIPTOR_HASH_INDEX.id();
    }

    @Override
    public String toString() {
        return S.toString(CatalogHashIndexDescriptor.class, this, super.toString());
    }
}
