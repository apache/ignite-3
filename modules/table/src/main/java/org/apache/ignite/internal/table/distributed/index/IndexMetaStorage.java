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

package org.apache.ignite.internal.table.distributed.index;

import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_AVAILABLE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_BUILDING;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_REMOVED;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_STOPPING;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_ALTER;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatusEnum.READ_ONLY;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.IndexEventParameters;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StoppingIndexEventParameters;
import org.apache.ignite.internal.catalog.events.TableEventParameters;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.vault.VaultManager;
import org.jetbrains.annotations.Nullable;

/**
 * {@link IndexMeta Index meta} storage, and is also responsible for their life cycle.
 *
 * @see IndexMeta
 */
// TODO: IGNITE-22367 реализовать
public class IndexMetaStorage implements IgniteComponent {
    private final CatalogService catalogService;

    private final LowWatermark lowWatermark;

    private final VaultManager vaultManager;

    private final Map<Integer, IndexMeta> indexMetaByIndexId = new ConcurrentHashMap<>();

    /** Constructor. */
    public IndexMetaStorage(
            CatalogService catalogService,
            LowWatermark lowWatermark,
            VaultManager vaultManager
    ) {
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
        this.vaultManager = vaultManager;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        catalogService.listen(INDEX_CREATE, fromConsumer(this::onIndexCreate));
        catalogService.listen(INDEX_REMOVED, fromConsumer(this::onIndexCreate));
        catalogService.listen(INDEX_BUILDING, fromConsumer(this::onIndexBuilding));
        catalogService.listen(INDEX_AVAILABLE, fromConsumer(this::onIndexAvailable));
        catalogService.listen(INDEX_STOPPING, fromConsumer(this::onIndexStopping));

        catalogService.listen(TABLE_ALTER, fromConsumer(this::onTableAlter));

        // TODO: IGNITE-22367 а также сделать восстановление изменений

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    /**
     * Returns the index meta if exists.
     *
     * @param indexId Index ID.
     */
    public @Nullable IndexMeta indexMeta(int indexId) {
        return indexMetaByIndexId.get(indexId);
    }

    private void onIndexCreate(CreateIndexEventParameters parameters) {
        CatalogIndexDescriptor catalogIndexDescriptor = parameters.indexDescriptor();

        Catalog catalog = catalog(parameters.catalogVersion());

        indexMetaByIndexId.compute(catalogIndexDescriptor.id(), (indexId, indexMeta) -> {
            assert indexMeta == null : indexId;

            return new IndexMeta(catalogIndexDescriptor, catalog);
        });
    }

    private void onIndexRemoved(RemoveIndexEventParameters parameters) {
        int indexId = parameters.indexId();

        Catalog catalog = catalog(parameters.catalogVersion());

        // TODO: IGNITE-22367 вот тут надо защититься от обновления лвм параллельно

        indexMetaByIndexId.compute(indexId, (id, indexMeta) -> {
            assert indexMeta != null : indexId;

            return setNewStatus(indexMeta, READ_ONLY, catalog);
        });
    }

    private void onIndexBuilding(StartBuildingIndexEventParameters parameters) {
        setNewStatus(parameters);
    }

    private void onIndexAvailable(MakeIndexAvailableEventParameters parameters) {
        setNewStatus(parameters);
    }

    private void onIndexStopping(StoppingIndexEventParameters parameters) {
        setNewStatus(parameters);
    }

    private void onTableAlter(TableEventParameters parameters) {
        if (parameters instanceof RenameTableEventParameters) {
            int catalogVersion = parameters.catalogVersion();

            CatalogTableDescriptor catalogTableDescriptor = catalogService.table(parameters.tableId(), catalogVersion);

            assert catalogTableDescriptor != null : "tableId=" + parameters.tableId() + ", catalogVersion=" + catalogVersion;

            int indexId = catalogTableDescriptor.primaryKeyIndexId();

            CatalogIndexDescriptor catalogIndexDescriptor = catalogIndexDescriptor(indexId, catalogVersion);

            indexMetaByIndexId.compute(indexId, (id, indexMeta) -> {
                assert indexMeta != null : indexId;

                return indexMeta.indexName(catalogIndexDescriptor.name());
            });
        }
    }

    private CatalogIndexDescriptor catalogIndexDescriptor(int indexId, int catalogVersion) {
        CatalogIndexDescriptor catalogIndexDescriptor = catalogService.index(indexId, catalogVersion);

        assert catalogIndexDescriptor != null : "indexId=" + indexId + ", catalogVersion=" + catalogVersion;

        return catalogIndexDescriptor;
    }

    private Catalog catalog(int catalogVersion) {
        Catalog catalog = catalogService.catalog(catalogVersion);

        assert catalog != null : catalogVersion;

        return catalog;
    }

    private void setNewStatus(IndexEventParameters parameters) {
        CatalogIndexDescriptor catalogIndexDescriptor = catalogIndexDescriptor(parameters.indexId(), parameters.catalogVersion());

        Catalog catalog = catalog(parameters.catalogVersion());

        indexMetaByIndexId.compute(catalogIndexDescriptor.id(), (indexId, indexMeta) -> {
            assert indexMeta != null : indexId;

            return setNewStatus(indexMeta, MetaIndexStatusEnum.convert(catalogIndexDescriptor.status()), catalog);
        });
    }

    private static IndexMeta setNewStatus(IndexMeta indexMeta, MetaIndexStatusEnum newStatus, Catalog catalog) {
        return indexMeta.status(newStatus, catalog.version(), catalog.time());
    }
}
