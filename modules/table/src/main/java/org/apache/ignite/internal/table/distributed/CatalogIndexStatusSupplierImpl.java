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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_AVAILABLE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_BUILDING;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_REMOVED;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_STOPPING;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent.LOW_WATERMARK_CHANGED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.IndexEventParameters;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StoppingIndexEventParameters;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.apache.ignite.internal.table.LongPriorityQueue;
import org.jetbrains.annotations.Nullable;

/** Implementation with caching of index statuses. */
// TODO: IGNITE-21608 Don’t forget to check if, as a result of fixing issues with catalog compaction, it will be possible to use a different
//  implementation
public class CatalogIndexStatusSupplierImpl implements CatalogIndexStatusSupplier, IgniteComponent {
    private final CatalogService catalogService;

    private final LowWatermark lowWatermark;

    private final Map<Integer, CatalogIndexStatus> statusByIndexId = new ConcurrentHashMap<>();

    private final LongPriorityQueue<DropIndexStatusEvent> dropIndexStatusEventQueue = new LongPriorityQueue<>(
            DropIndexStatusEvent::catalogVersion
    );

    /** Constructor. */
    public CatalogIndexStatusSupplierImpl(CatalogService catalogService, LowWatermark lowWatermark) {
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
    }

    @Override
    public CompletableFuture<Void> start() {
        recoverStructures();

        addListeners();

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        statusByIndexId.clear();
    }

    @Override
    public @Nullable CatalogIndexStatus get(int indexId) {
        return statusByIndexId.get(indexId);
    }

    private void recoverStructures() {
        // LWM starts updating only after the node is restored.
        HybridTimestamp lwm = lowWatermark.getLowWatermark();

        int earliestCatalogVersion = catalogService.activeCatalogVersion(hybridTimestampToLong(lwm));
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        // First we add all live indexes.
        catalogService.indexes(latestCatalogVersion).forEach(this::putIndexStatus);

        // Let's fill the queue for destroying the index status.
        for (int catalogVersion = latestCatalogVersion - 1; catalogVersion >= earliestCatalogVersion; catalogVersion--) {
            for (CatalogIndexDescriptor index : catalogService.indexes(catalogVersion)) {
                int indexId = index.id();

                if (statusByIndexId.putIfAbsent(indexId, index.status()) == null) {
                    dropIndexStatusEventQueue.enqueue(new DropIndexStatusEvent(indexId, catalogVersion + 1));
                }
            }
        }
    }

    private void addListeners() {
        catalogService.listen(
                INDEX_CREATE,
                fromConsumer((CreateIndexEventParameters parameters) -> putIndexStatus(parameters.indexDescriptor()))
        );
        catalogService.listen(INDEX_BUILDING, fromConsumer((StartBuildingIndexEventParameters parameters) -> putIndexStatus(parameters)));
        catalogService.listen(INDEX_AVAILABLE, fromConsumer((MakeIndexAvailableEventParameters parameters) -> putIndexStatus(parameters)));
        catalogService.listen(INDEX_STOPPING, fromConsumer((StoppingIndexEventParameters parameters) -> putIndexStatus(parameters)));
        catalogService.listen(INDEX_REMOVED, fromConsumer(this::onIndexRemoved));

        lowWatermark.listen(LOW_WATERMARK_CHANGED, fromConsumer(this::onLwmChanged));
    }

    private void onIndexRemoved(RemoveIndexEventParameters parameters) {
        int indexId = parameters.indexId();
        int catalogVersion = parameters.catalogVersion();

        lowWatermark.getLowWatermarkSafe(lwm -> {
            int lwmCatalogVersion = catalogService.activeCatalogVersion(hybridTimestampToLong(lwm));

            if (catalogVersion <= lwmCatalogVersion) {
                // We can delete it immediately since the index will be destroyed on updating the low watermark.
                statusByIndexId.remove(indexId);
            } else {
                dropIndexStatusEventQueue.enqueue(new DropIndexStatusEvent(indexId, catalogVersion));
            }
        });
    }

    private void onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        int lwmCatalogVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());

        List<DropIndexStatusEvent> events = dropIndexStatusEventQueue.drainUpTo(lwmCatalogVersion);

        events.forEach(event -> statusByIndexId.remove(event.indexId()));
    }

    private void putIndexStatus(CatalogIndexDescriptor index) {
        statusByIndexId.put(index.id(), index.status());
    }

    private void putIndexStatus(IndexEventParameters parameters) {
        putIndexStatus(index(parameters.indexId(), parameters.catalogVersion()));
    }

    private CatalogIndexDescriptor index(int indexId, int catalogVersion) {
        CatalogIndexDescriptor index = catalogService.index(indexId, catalogVersion);

        assert index != null : "indexId=" + indexId + ", catalogVersion=" + catalogVersion;

        return index;
    }

    private static class DropIndexStatusEvent {
        private final int indexId;

        private final int catalogVersion;

        private DropIndexStatusEvent(int indexId, int catalogVersion) {
            this.indexId = indexId;
            this.catalogVersion = catalogVersion;
        }

        public int indexId() {
            return indexId;
        }

        public int catalogVersion() {
            return catalogVersion;
        }
    }
}
