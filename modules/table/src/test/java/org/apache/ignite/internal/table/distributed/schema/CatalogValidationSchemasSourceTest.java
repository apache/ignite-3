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

package org.apache.ignite.internal.table.distributed.schema;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.AdditionalMatchers.lt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CatalogValidationSchemasSourceTest extends BaseIgniteAbstractTest {
    @Mock
    private CatalogService catalogService;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private SchemaRegistry schemaRegistry;

    @InjectMocks
    private CatalogValidationSchemasSource schemas;

    private final HybridClock clock = new HybridClockImpl();

    @Test
    void waitingForSchemaAvailabilityAtVersionWorks() {
        int tableId = 1;
        int version = 3;
        CompletableFuture<SchemaDescriptor> underlyingFuture = new CompletableFuture<>();

        doReturn(schemaRegistry).when(schemaManager).schemaRegistry(tableId);
        when(schemaRegistry.schemaAsync(version)).thenReturn(underlyingFuture);

        CompletableFuture<Void> future = schemas.waitForSchemaAvailability(tableId, version);
        assertThat(future, is(not(completedFuture())));

        underlyingFuture.complete(mock(SchemaDescriptor.class));

        assertThat(future, is(completedFuture()));
    }

    @Test
    void tableSchemaVersionsBetweenTimestampsWorks() {
        int tableId = 1;

        CatalogTableDescriptor version3 = tableVersion(tableId, 3);
        CatalogTableDescriptor version4 = tableVersion(tableId, 4);

        HybridTimestamp from = clock.now();
        HybridTimestamp to = clock.now();

        when(catalogService.activeCatalogVersion(from.longValue())).thenReturn(3);
        when(catalogService.activeCatalogVersion(to.longValue())).thenReturn(4);
        when(catalogService.table(tableId, 3)).thenReturn(version3);
        when(catalogService.table(tableId, 4)).thenReturn(version4);

        List<FullTableSchema> fullSchemas = schemas.tableSchemaVersionsBetween(tableId, from, to);

        assertThat(fullSchemas, hasSize(2));
        assertThat(fullSchemas.get(0).schemaVersion(), is(3));
        assertThat(fullSchemas.get(1).schemaVersion(), is(4));
    }

    private static CatalogTableDescriptor tableVersion(int tableId, int tableVersion) {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("k1", ColumnType.INT16, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("v1", ColumnType.INT32, false, 0, 0, 0, null)
        );

        CatalogTableDescriptor descriptor = new CatalogTableDescriptor(
                tableId, -1, -1, "test", 0, columns, List.of("k1"), null, DEFAULT_STORAGE_PROFILE
        );

        for (int ver = CatalogTableDescriptor.INITIAL_TABLE_VERSION + 1; ver <= tableVersion; ver++) {
            descriptor = descriptor.newDescriptor(
                    "test", ver, columns, CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN, DEFAULT_STORAGE_PROFILE);
        }

        return descriptor;
    }

    @Test
    void tableSchemaVersionsBetweenTimestampsUsesCache() {
        int tableId = 1;

        CatalogTableDescriptor version3 = tableVersion(tableId, 3);
        CatalogTableDescriptor version4 = tableVersion(tableId, 4);

        HybridTimestamp timestamp = clock.now();

        when(catalogService.activeCatalogVersion(lt(timestamp.longValue()))).thenReturn(3);
        when(catalogService.activeCatalogVersion(geq(timestamp.longValue()))).thenReturn(4);
        when(catalogService.table(tableId, 3)).thenReturn(version3);
        when(catalogService.table(tableId, 4)).thenReturn(version4);

        List<FullTableSchema> fullSchemas1 = schemas.tableSchemaVersionsBetween(tableId, timestamp.subtractPhysicalTime(2), timestamp);
        List<FullTableSchema> fullSchemas2 = schemas.tableSchemaVersionsBetween(
                tableId,
                timestamp.subtractPhysicalTime(1),
                timestamp.addPhysicalTime(10)
        );

        assertThat(fullSchemas1.size(), is(fullSchemas2.size()));

        verify(catalogService, times(1)).table(tableId, 3);
    }

    @Test
    void tableSchemaVersionsBetweenTimestampAndVersionWorks() {
        int tableId = 1;

        CatalogTableDescriptor version3 = tableVersion(tableId, 3);
        CatalogTableDescriptor version4 = tableVersion(tableId, 4);
        CatalogTableDescriptor version5 = tableVersion(tableId, 5);

        HybridTimestamp from = clock.now();

        when(catalogService.latestCatalogVersion()).thenReturn(5);
        when(catalogService.activeCatalogVersion(from.longValue())).thenReturn(3);
        when(catalogService.table(tableId, 3)).thenReturn(version3);
        when(catalogService.table(tableId, 4)).thenReturn(version4);
        when(catalogService.table(tableId, 5)).thenReturn(version5);

        List<FullTableSchema> fullSchemas = schemas.tableSchemaVersionsBetween(tableId, from, 4);

        assertThat(fullSchemas, hasSize(2));
        assertThat(fullSchemas.get(0).schemaVersion(), is(3));
        assertThat(fullSchemas.get(1).schemaVersion(), is(4));
    }

    @Test
    void tableSchemaVersionsBetweenTimestampAndVersionReturnsEmptyListIfEndIsBeforeStart() {
        int tableId = 1;

        CatalogTableDescriptor version3 = tableVersion(tableId, 3);

        HybridTimestamp from = clock.now();

        when(catalogService.latestCatalogVersion()).thenReturn(3);
        when(catalogService.activeCatalogVersion(from.longValue())).thenReturn(3);
        when(catalogService.table(tableId, 3)).thenReturn(version3);

        List<FullTableSchema> fullSchemas = schemas.tableSchemaVersionsBetween(tableId, from, 2);

        assertThat(fullSchemas, is(empty()));
    }

    @Test
    void tableSchemaVersionsBetweenTimestampAndVersionUsesCache() {
        int tableId = 1;

        CatalogTableDescriptor version3 = tableVersion(tableId, 3);
        CatalogTableDescriptor version4 = tableVersion(tableId, 4);

        HybridTimestamp timestamp = clock.now();

        when(catalogService.latestCatalogVersion()).thenReturn(4);
        when(catalogService.activeCatalogVersion(anyLong())).thenReturn(3);
        when(catalogService.table(tableId, 3)).thenReturn(version3);
        when(catalogService.table(tableId, 4)).thenReturn(version4);

        List<FullTableSchema> fullSchemas1 = schemas.tableSchemaVersionsBetween(tableId, timestamp.subtractPhysicalTime(2), 4);
        List<FullTableSchema> fullSchemas2 = schemas.tableSchemaVersionsBetween(tableId, timestamp.subtractPhysicalTime(1), 4);

        assertThat(fullSchemas1.size(), is(fullSchemas2.size()));

        verify(catalogService, times(1)).table(tableId, 3);
    }
}
