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

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.AlterColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes a column replacement.
 */
public class AlterColumnEntry implements UpdateEntry, Fireable {
    private static final long serialVersionUID = -4552940987881338656L;

    private final int tableId;

    private final CatalogTableColumnDescriptor column;

    private final String schemaName;

    /**
     * Constructs the object.
     *
     * @param tableId An id the table to be modified.
     * @param column A modified descriptor of the column to be replaced.
     * @param schemaName Schema name.
     */
    public AlterColumnEntry(int tableId, CatalogTableColumnDescriptor column, String schemaName) {
        this.tableId = tableId;
        this.column = column;
        this.schemaName = schemaName;
    }

    /** Returns an id the table to be modified. */
    public int tableId() {
        return tableId;
    }

    /** Returns a descriptor for the column to be replaced. */
    public CatalogTableColumnDescriptor descriptor() {
        return column;
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
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName));

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                CatalogUtils.replaceSchema(new CatalogSchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        Arrays.stream(schema.tables())
                                .map(table -> table.id() != tableId
                                        ? table
                                        : new CatalogTableDescriptor(
                                                table.id(),
                                                table.primaryKeyIndexId(),
                                                table.name(),
                                                table.zoneId(),
                                                table.tableVersion() + 1,
                                                table.columns().stream()
                                                        .map(source -> source.name().equals(column.name()) ? column : source)
                                                        .collect(toList()),
                                                table.primaryKeyColumns(),
                                                table.colocationColumns(),
                                                causalityToken)
                                )
                                .toArray(CatalogTableDescriptor[]::new),
                        schema.indexes(),
                        schema.systemViews(),
                        causalityToken
                ), catalog.schemas())
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
