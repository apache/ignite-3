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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultZoneIdOpt;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.CollectionUtils;

/**
 * New schema entry.
 */
public class NewSchemaEntry implements UpdateEntry {
    private final CatalogSchemaDescriptor descriptor;

    public NewSchemaEntry(CatalogSchemaDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    public CatalogSchemaDescriptor descriptor() {
        return descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public Catalog applyUpdate(Catalog catalog, HybridTimestamp timestamp) {
        descriptor.updateTimestamp(timestamp);

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                CollectionUtils.concat(catalog.schemas(), List.of(descriptor)),
                defaultZoneIdOpt(catalog)
        );
    }

    /** {@inheritDoc} */
    @Override
    public int typeId() {
        return MarshallableEntryType.NEW_SCHEMA.id();
    }
}
