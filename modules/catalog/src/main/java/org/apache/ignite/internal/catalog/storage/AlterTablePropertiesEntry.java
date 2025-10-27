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

import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor.Builder;
import org.apache.ignite.internal.catalog.events.AlterTablePropertiesEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Describes changing of table properties.
 */
public class AlterTablePropertiesEntry extends AbstractUpdateTableEntry implements Fireable {
    private final int tableId;

    private final @Nullable Double staleRowsFraction;
    private final @Nullable Long minStaleRowsCount;

    /**
     * Constructs the object.
     *
     * @param tableId Table id.
     * @param staleRowsFraction Stale rows fraction.
     * @param minStaleRowsCount Minimum stale rows count
     */
    public AlterTablePropertiesEntry(
            int tableId,
            @Nullable Double staleRowsFraction,
            @Nullable Long minStaleRowsCount
    ) {
        this.tableId = tableId;
        this.staleRowsFraction = staleRowsFraction;
        this.minStaleRowsCount = minStaleRowsCount;
    }

    /** Returns table id. */
    @Override
    public int tableId() {
        return tableId;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.ALTER_TABLE_PROPERTIES.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.TABLE_ALTER;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new AlterTablePropertiesEventParameters(causalityToken, catalogVersion, this);
    }

    @Override
    public Builder newTableDescriptor(CatalogTableDescriptor table) {
        Builder builder = table.copyBuilder();

        if (minStaleRowsCount != null) {
            builder.minStaleRowsCount(minStaleRowsCount);
        }

        if (staleRowsFraction != null) {
            builder.staleRowsFraction(staleRowsFraction);
        }

        return builder;
    }

    public @Nullable Double staleRowsFraction() {
        return staleRowsFraction;
    }

    public @Nullable Long minStaleRowsCount() {
        return minStaleRowsCount;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
