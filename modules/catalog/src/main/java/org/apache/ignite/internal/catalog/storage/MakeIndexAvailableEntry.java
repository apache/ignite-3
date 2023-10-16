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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Arrays;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;

/** Entry for {@link MakeIndexAvailableCommand}. */
public class MakeIndexAvailableEntry implements UpdateEntry, Fireable {
    private static final long serialVersionUID = -5686678143537999594L;

    private final int indexId;

    /** Constructor. */
    public MakeIndexAvailableEntry(int indexId) {
        this.indexId = indexId;
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
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
                                .map(source -> source.id() == indexId ? createReadWriteIndex(source, causalityToken) : source)
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
        return new MakeIndexAvailableEventParameters(causalityToken, catalogVersion, indexId);
    }

    private CatalogIndexDescriptor createReadWriteIndex(CatalogIndexDescriptor source, long causalityToken) {
        CatalogIndexDescriptor updateIndexDescriptor;

        if (source instanceof CatalogHashIndexDescriptor) {
            updateIndexDescriptor = createReadWriteIndex((CatalogHashIndexDescriptor) source);
        } else if (source instanceof CatalogSortedIndexDescriptor) {
            updateIndexDescriptor = createReadWriteIndex((CatalogSortedIndexDescriptor) source);
        } else {
            throw new CatalogValidationException(format("Unsupported index type '{}' {}", indexId, source));
        }

        updateIndexDescriptor.updateToken(causalityToken);

        return updateIndexDescriptor;
    }

    private static CatalogIndexDescriptor createReadWriteIndex(CatalogHashIndexDescriptor index) {
        return new CatalogHashIndexDescriptor(
                index.id(),
                index.name(),
                index.tableId(),
                index.unique(),
                index.columns(),
                false
        );
    }

    private static CatalogIndexDescriptor createReadWriteIndex(CatalogSortedIndexDescriptor index) {
        return new CatalogSortedIndexDescriptor(
                index.id(),
                index.name(),
                index.tableId(),
                index.unique(),
                index.columns(),
                false
        );
    }
}
