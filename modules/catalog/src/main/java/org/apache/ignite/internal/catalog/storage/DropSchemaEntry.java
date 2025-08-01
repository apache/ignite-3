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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultZoneIdOpt;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes deletion of a schema.
 */
public class DropSchemaEntry implements UpdateEntry {
    private final int schemaId;

    /**
     * Constructs the object.
     *
     * @param schemaId An id of a schema to drop.
     */
    public DropSchemaEntry(int schemaId) {
        this.schemaId = schemaId;
    }

    /** Returns an id of a schema to drop. */
    public int schemaId() {
        return schemaId;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DROP_SCHEMA.id();
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, HybridTimestamp timestamp) {
        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                catalog.schemas().stream().filter(s -> s.id() != schemaId).collect(toList()),
                defaultZoneIdOpt(catalog)
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
