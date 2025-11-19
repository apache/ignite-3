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
import java.util.Set;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor.Builder;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes dropping of columns.
 */
public class DropColumnsEntry extends AbstractUpdateTableEntry implements Fireable {
    private final int tableId;
    private final Set<String> columns;

    /**
     * Constructs the object.
     *
     * @param tableId Table id.
     * @param columns Names of columns to drop.
     */
    public DropColumnsEntry(int tableId, Set<String> columns) {
        this.tableId = tableId;
        this.columns = columns;
    }

    /** Returns table id. */
    @Override
    public int tableId() {
        return tableId;
    }

    /** Returns name of columns to drop. */
    public Set<String> columns() {
        return columns;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DROP_COLUMN.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.TABLE_ALTER;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new DropColumnEventParameters(causalityToken, catalogVersion, tableId, columns);
    }

    @Override
    public Builder newTableDescriptor(CatalogTableDescriptor table) {
        List<CatalogTableColumnDescriptor> updatedTableColumns = table.columns().stream()
                .filter(col -> !columns.contains(col.name()))
                .collect(toList());

        return table.copyBuilder()
                .newColumns(updatedTableColumns);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
