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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.indexOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceIndex;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.replaceSchema;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;

/** Abstract entry for changing {@link CatalogIndexDescriptor#status() index status}. */
abstract class AbstractChangeIndexStatusEntry implements UpdateEntry {
    protected final int indexId;

    private final CatalogIndexStatus newStatus;

    /** Constructor. */
    AbstractChangeIndexStatusEntry(int indexId, CatalogIndexStatus newStatus) {
        this.indexId = indexId;
        this.newStatus = newStatus;
    }

    @Override
    public final Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor schema = schemaByIndexId(catalog, indexId);

        CatalogIndexDescriptor newIndexDescriptor = updateIndexStatus(catalog, causalityToken, newStatus);

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                replaceSchema(replaceIndex(schema, newIndexDescriptor), catalog.schemas()),
                catalog.defaultZone().id()
        );
    }

    static CatalogSchemaDescriptor schemaByIndexId(Catalog catalog, int indexId) {
        CatalogIndexDescriptor index = catalog.index(indexId);

        assert index != null : indexId;

        CatalogTableDescriptor table = catalog.table(index.tableId());

        assert table != null : index.tableId();

        CatalogSchemaDescriptor schema = catalog.schema(table.schemaId());

        assert schema != null : table.schemaId();

        return schema;
    }

    private CatalogIndexDescriptor updateIndexStatus(
            Catalog catalog,
            long causalityToken,
            CatalogIndexStatus newStatus
    ) {
        CatalogIndexDescriptor source = indexOrThrow(catalog, indexId);

        // We only care about the transitions to REGISTERED and STOPPING. REGISTERED status has already been handled on index creation.
        int txWaitCatalogVersion = newStatus == CatalogIndexStatus.STOPPING ? catalog.version() + 1 : source.txWaitCatalogVersion();

        CatalogIndexDescriptor updateIndexDescriptor;

        if (source instanceof CatalogHashIndexDescriptor) {
            updateIndexDescriptor = updateHashIndexStatus((CatalogHashIndexDescriptor) source, newStatus, txWaitCatalogVersion);
        } else if (source instanceof CatalogSortedIndexDescriptor) {
            updateIndexDescriptor = updateSortedIndexStatus((CatalogSortedIndexDescriptor) source, newStatus, txWaitCatalogVersion);
        } else {
            throw new CatalogValidationException(format("Unsupported index type '{}' {}", source.id(), source));
        }

        updateIndexDescriptor.updateToken(causalityToken);

        return updateIndexDescriptor;
    }

    private static CatalogIndexDescriptor updateHashIndexStatus(
            CatalogHashIndexDescriptor index, CatalogIndexStatus newStatus, int txWaitCatalogVersion
    ) {
        return new CatalogHashIndexDescriptor(
                index.id(),
                index.name(),
                index.tableId(),
                index.unique(),
                newStatus,
                txWaitCatalogVersion,
                index.zoneId(),
                index.columns()
        );
    }

    private static CatalogIndexDescriptor updateSortedIndexStatus(
            CatalogSortedIndexDescriptor index, CatalogIndexStatus newStatus, int txWaitCatalogVersion
    ) {
        return new CatalogSortedIndexDescriptor(
                index.id(),
                index.name(),
                index.tableId(),
                index.unique(),
                newStatus,
                txWaitCatalogVersion,
                index.zoneId(),
                index.columns()
        );
    }
}
