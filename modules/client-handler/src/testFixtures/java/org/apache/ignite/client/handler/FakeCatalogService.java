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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

    private final Catalog catalog;

    /**
     * Creates fake catalog service.
     */
    public FakeCatalogService(int partitions) {
        catalog = mock(Catalog.class);

        lenient().doAnswer(invocation -> new CatalogTableDescriptor(
                invocation.getArgument(0),
                0,
                0,
                "table",
                invocation.<Integer>getArgument(0) + 10_000,
                List.of(mock(CatalogTableColumnDescriptor.class)),
                List.of(),
                null,
                DEFAULT_STORAGE_PROFILE)
        ).when(catalog).table(anyInt());

        lenient().doAnswer(invocation -> new CatalogZoneDescriptor(
                invocation.getArgument(0),
                "zone",
                partitions,
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
    public int latestCatalogVersion() {
        return 0;
    }

    @Override
    public CompletableFuture<Void> catalogReadyFuture(int version) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void listen(CatalogEvent evt, EventListener<? extends CatalogEventParameters> listener) {

    }

    @Override
    public void removeListener(CatalogEvent evt, EventListener<? extends CatalogEventParameters> listener) {

    }
}
