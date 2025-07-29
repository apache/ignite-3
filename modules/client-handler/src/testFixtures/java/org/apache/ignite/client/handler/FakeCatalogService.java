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

package org.apache.ignite.client.handler;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.ToIntFunction;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.event.EventListener;

/**
 * Fake catalog service.
 */
public class FakeCatalogService implements CatalogService {
    /** Catalog mock. */
    private final Catalog catalog;

    private final ToIntFunction<Integer> zoneIdProvider;

    /**
     * Creates fake catalog service.
     *
     * @param partitions Amount of partitions for a zone.
     */
    public FakeCatalogService(int partitions) {
        this(partitions, tableId -> tableId + 10_000);
    }

    /**
     * Creates fake catalog service.
     *
     * @param partitions Amount of partitions for a zone.
     * @param zoneIdProvider Function that provides the zone id given a table id.
     */
    public FakeCatalogService(int partitions, ToIntFunction<Integer> zoneIdProvider) {
        this.zoneIdProvider = zoneIdProvider;

        catalog = mock(Catalog.class);

        lenient().doAnswer(invocation -> {
            int tableId = invocation.getArgument(0);

            return new CatalogTableDescriptor(
                    tableId,
                    0,
                    0,
                    "table",
                    zoneIdProvider.applyAsInt(tableId),
                    List.of(mock(CatalogTableColumnDescriptor.class)),
                    List.of(),
                    null,
                    DEFAULT_STORAGE_PROFILE
            );
        }).when(catalog).table(anyInt());

        lenient().doAnswer(invocation -> new CatalogZoneDescriptor(
                invocation.getArgument(0),
                "zone",
                partitions,
                0,
                0,
                0,
                0,
                0,
                "",
                fromParams(Collections.emptyList()),
                ConsistencyMode.STRONG_CONSISTENCY
        )).when(catalog).zone(anyInt());
    }

    @Override
    public Catalog catalog(int catalogVersion) {
        return catalog;
    }

    @Override
    public Catalog activeCatalog(long timestamp) {
        return catalog;
    }

    @Override
    public int activeCatalogVersion(long timestamp) {
        return 0;
    }

    @Override
    public int earliestCatalogVersion() {
        return 0;
    }

    @Override
    public Catalog earliestCatalog() {
        return catalog;
    }

    @Override
    public int latestCatalogVersion() {
        return 0;
    }

    @Override
    public CompletableFuture<Void> catalogReadyFuture(int version) {
        return nullCompletedFuture();
    }

    @Override
    public void listen(CatalogEvent evt, EventListener<? extends CatalogEventParameters> listener) {
    }

    @Override
    public void removeListener(CatalogEvent evt, EventListener<? extends CatalogEventParameters> listener) {
    }
}
