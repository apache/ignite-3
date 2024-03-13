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
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.event.EventListener;

/**
 * Fake catalog service.
 */
public class FakeCatalogService implements CatalogService {
    private final int partitions;

    public FakeCatalogService(int partitions) {
        this.partitions = partitions;
    }

    @Override
    public Catalog catalog(int catalogVersion) {
        return null;
    }

    @Override
    public CatalogTableDescriptor table(String tableName, long timestamp) {
        return null;
    }

    @Override
    public CatalogTableDescriptor table(int tableId, long timestamp) {
        return new CatalogTableDescriptor(
                tableId, 0, 0, "table", 0, List.of(mock(CatalogTableColumnDescriptor.class)), List.of(), null, DEFAULT_STORAGE_PROFILE);
    }

    @Override
    public CatalogTableDescriptor table(int tableId, int catalogVersion) {
        return null;
    }

    @Override
    public Collection<CatalogTableDescriptor> tables(int catalogVersion) {
        return null;
    }

    @Override
    public CatalogIndexDescriptor aliveIndex(String indexName, long timestamp) {
        return null;
    }

    @Override
    public CatalogIndexDescriptor index(int indexId, long timestamp) {
        return null;
    }

    @Override
    public CatalogIndexDescriptor index(int indexId, int catalogVersion) {
        return null;
    }

    @Override
    public Collection<CatalogIndexDescriptor> indexes(int catalogVersion) {
        return null;
    }

    @Override
    public List<CatalogIndexDescriptor> indexes(int catalogVersion, int tableId) {
        return null;
    }

    @Override
    public CatalogSchemaDescriptor schema(int catalogVersion) {
        return null;
    }

    @Override
    public CatalogSchemaDescriptor schema(String schemaName, int catalogVersion) {
        return null;
    }

    @Override
    public CatalogSchemaDescriptor schema(int schemaId, int catalogVersion) {
        return null;
    }

    @Override
    public CatalogZoneDescriptor zone(String zoneName, long timestamp) {
        return null;
    }

    @Override
    public CatalogZoneDescriptor zone(int zoneId, long timestamp) {
        return new CatalogZoneDescriptor(zoneId, "zone", partitions, 0, 0, 0, 0, "", fromParams(Collections.emptyList()));
    }

    @Override
    public CatalogZoneDescriptor zone(int zoneId, int catalogVersion) {
        return null;
    }

    @Override
    public Collection<CatalogZoneDescriptor> zones(int catalogVersion) {
        return null;
    }

    @Override
    public CatalogSchemaDescriptor activeSchema(long timestamp) {
        return null;
    }

    @Override
    public CatalogSchemaDescriptor activeSchema(String schemaName, long timestamp) {
        return null;
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
        return null;
    }

    @Override
    public void listen(CatalogEvent evt, EventListener<? extends CatalogEventParameters> listener) {

    }

    @Override
    public void removeListener(CatalogEvent evt, EventListener<? extends CatalogEventParameters> listener) {

    }
}
