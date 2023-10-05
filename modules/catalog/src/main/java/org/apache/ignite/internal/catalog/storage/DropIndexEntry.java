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

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes deletion of an index.
 */
public class DropIndexEntry implements UpdateEntry, Fireable {
    private static final long serialVersionUID = -604729846502020728L;

    private final int indexId;

    private final int tableId;

    private final String schemaName;

    /**
     * Constructs the object.
     *
     * @param indexId An id of an index to drop.
     * @param tableId Table ID for which the index was removed.
     * @param schemaName Schema name.
     */
    public DropIndexEntry(int indexId, int tableId, String schemaName) {
        this.indexId = indexId;
        this.tableId = tableId;
        this.schemaName = schemaName;
    }

    /** Returns an id of an index to drop. */
    public int indexId() {
        return indexId;
    }

    /** Returns table ID for which the index was removed. */
    public int tableId() {
        return tableId;
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_DROP;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new DropIndexEventParameters(causalityToken, catalogVersion, indexId, tableId);
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
                        schema.tables(),
                        Arrays.stream(schema.indexes()).filter(t -> t.id() != indexId).toArray(CatalogIndexDescriptor[]::new),
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
