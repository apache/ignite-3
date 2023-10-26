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
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
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
        CatalogSchemaDescriptor schema = schemaByIndexId(catalog, indexId);

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
                                .map(source -> source.id() == indexId ? createAvailableIndex(source, causalityToken) : source)
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

    private static CatalogSchemaDescriptor schemaByIndexId(Catalog catalog, int indexId) {
        CatalogIndexDescriptor index = catalog.index(indexId);

        assert index != null : indexId;

        CatalogTableDescriptor table = catalog.table(index.tableId());

        assert table != null : index.tableId();

        CatalogSchemaDescriptor schema = catalog.schema(table.schemaId());

        assert schema != null : table.schemaId();

        return schema;
    }

    private static CatalogIndexDescriptor createAvailableIndex(CatalogIndexDescriptor source, long causalityToken) {
        CatalogIndexDescriptor updateIndexDescriptor;

        if (source instanceof CatalogHashIndexDescriptor) {
            updateIndexDescriptor = createAvailableIndex((CatalogHashIndexDescriptor) source);
        } else if (source instanceof CatalogSortedIndexDescriptor) {
            updateIndexDescriptor = createAvailableIndex((CatalogSortedIndexDescriptor) source);
        } else {
            throw new CatalogValidationException(format("Unsupported index type '{}' {}", source.id(), source));
        }

        updateIndexDescriptor.updateToken(causalityToken);

        return updateIndexDescriptor;
    }

    private static CatalogIndexDescriptor createAvailableIndex(CatalogHashIndexDescriptor index) {
        return new CatalogHashIndexDescriptor(
                index.id(),
                index.name(),
                index.tableId(),
                index.unique(),
                index.columns(),
                true
        );
    }

    private static CatalogIndexDescriptor createAvailableIndex(CatalogSortedIndexDescriptor index) {
        return new CatalogSortedIndexDescriptor(
                index.id(),
                index.name(),
                index.tableId(),
                index.unique(),
                index.columns(),
                true
        );
    }
}
