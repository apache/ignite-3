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

package org.apache.ignite.internal.catalog.storage;

import static java.util.stream.Collectors.toList;

import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor.Builder;
import org.apache.ignite.internal.catalog.events.AlterColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes a column replacement.
 */
public class AlterColumnEntry extends AbstractUpdateTableEntry implements Fireable {
    private final int tableId;

    private final CatalogTableColumnDescriptor column;

    /**
     * Constructs the object.
     *
     * @param tableId An id the table to be modified.
     * @param column A modified descriptor of the column to be replaced.
     */
    public AlterColumnEntry(int tableId, CatalogTableColumnDescriptor column) {
        this.tableId = tableId;
        this.column = column;
    }

    /** Returns an id the table to be modified. */
    @Override
    public int tableId() {
        return tableId;
    }

    /** Returns a descriptor for the column to be replaced. */
    public CatalogTableColumnDescriptor descriptor() {
        return column;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.ALTER_COLUMN.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.TABLE_ALTER;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new AlterColumnEventParameters(causalityToken, catalogVersion, tableId, column);
    }

    @Override
    public Builder newTableDescriptor(CatalogTableDescriptor table) {
        List<CatalogTableColumnDescriptor> updatedTableColumns = table.columns().stream()
                .map(source -> {
                    if (source.name().equals(column.name())) {
                        return column.clone(source.id());
                    }

                    return source;
                })
                .collect(toList());

        return table.copyBuilder()
                .newColumns(updatedTableColumns);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

}
