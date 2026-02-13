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

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.IndexEventParameters;
import org.apache.ignite.internal.catalog.events.TableEventParameters;
import org.apache.ignite.internal.util.CompletableFutures;

/**
 * Registers listeners for catalog events to track changes in tables dependencies.
 */
public class CatalogChangesListenerRegistrator {
    private final CatalogService catalogService;
    private final CatalogTableDependencyEventListener listener;

    CatalogChangesListenerRegistrator(CatalogService catalogService, CatalogTableDependencyEventListener listener) {
        this.catalogService = catalogService;
        this.listener = listener;
    }

    void registerListeners() {
        // TODO For testing purposes.
        if (catalogService == null) {
            return;
        }
        catalogService.listen(CatalogEvent.INDEX_CREATE, this::onIndexCreate);
        catalogService.listen(CatalogEvent.INDEX_AVAILABLE, this::onIndexEvent);
        catalogService.listen(CatalogEvent.INDEX_STOPPING, this::onIndexEvent);
        catalogService.listen(CatalogEvent.TABLE_ALTER, this::onTableAlter);
        catalogService.listen(CatalogEvent.TABLE_DROP, this::onTableDrop);
    }

    private CompletableFuture<Boolean> onTableDrop(CatalogEventParameters eventParameters) {
        assert eventParameters instanceof TableEventParameters : eventParameters;

        listener.onTableDrop(eventParameters.catalogVersion(), ((TableEventParameters) eventParameters).tableId());

        return CompletableFutures.falseCompletedFuture();
    }

    private CompletableFuture<Boolean> onTableAlter(CatalogEventParameters eventParameters) {
        assert eventParameters instanceof TableEventParameters : eventParameters;

        listener.onTableDependencyChanged(eventParameters.catalogVersion(), ((TableEventParameters) eventParameters).tableId());

        return CompletableFutures.falseCompletedFuture();
    }

    private CompletableFuture<Boolean> onIndexCreate(CatalogEventParameters eventParameters) {
        assert eventParameters instanceof CreateIndexEventParameters : eventParameters;

        listener.onTableDependencyChanged(
                eventParameters.catalogVersion(),
                ((CreateIndexEventParameters) eventParameters).indexDescriptor().tableId()
        );

        return CompletableFutures.falseCompletedFuture();
    }

    private CompletableFuture<Boolean> onIndexEvent(CatalogEventParameters eventParameters) {
        assert eventParameters instanceof IndexEventParameters : eventParameters;

        IndexEventParameters evtParameters = ((IndexEventParameters) eventParameters);

        CatalogIndexDescriptor index = catalogService.catalog(
                evtParameters.catalogVersion()).index(evtParameters.indexId());

        // TODO TableId should be included to event if possible.
        if (index != null) {
            int tableId = index.tableId();

            listener.onTableDependencyChanged(eventParameters.catalogVersion(), tableId);
        }

        return CompletableFutures.falseCompletedFuture();
    }
}
