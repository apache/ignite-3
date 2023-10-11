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
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.events.AvailableIndexEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;

/** Describes making an index read-write. */
public class MakeIndexAvailableEntry implements UpdateEntry, Fireable {
    private static final long serialVersionUID = -5686678143537999594L;

    private final String schemaName;

    private final CatalogIndexDescriptor descriptor;

    /** Constructor. */
    public MakeIndexAvailableEntry(String schemaName, CatalogIndexDescriptor descriptor) {
        this.schemaName = schemaName;
        this.descriptor = descriptor;
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        descriptor.updateToken(causalityToken);

        CatalogSchemaDescriptor schema = catalog.schema(schemaName);

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                CatalogUtils.replaceSchema(new CatalogSchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        schema.tables(),
                        Arrays.stream(schema.indexes())
                                .map(source -> source.id() == descriptor.id() ? descriptor : source)
                                .toArray(CatalogIndexDescriptor[]::new),
                        schema.systemViews(),
                        causalityToken
                ), catalog.schemas())
        );
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_AVAILABLE;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new AvailableIndexEventParameters(causalityToken, catalogVersion, descriptor.id());
    }

    /** Returns schema name. */
    public String schemaName() {
        return schemaName;
    }

    /** Returns descriptor of the available to read-write index. */
    public CatalogIndexDescriptor descriptor() {
        return descriptor;
    }
}
