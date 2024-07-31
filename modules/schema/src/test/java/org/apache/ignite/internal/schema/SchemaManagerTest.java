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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor.INITIAL_TABLE_VERSION;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SchemaManagerTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID = 3;
    private static final String TABLE_NAME = "t";

    private static final long CAUSALITY_TOKEN_1 = 0;
    private static final long CAUSALITY_TOKEN_2 = 45;

    private static final int CATALOG_VERSION_1 = 10;
    private static final int CATALOG_VERSION_2 = 11;

    private final AtomicReference<LongFunction<CompletableFuture<?>>> onMetastoreRevisionCompleteHolder = new AtomicReference<>();

    private final Consumer<LongFunction<CompletableFuture<?>>> registry = onMetastoreRevisionCompleteHolder::set;

    @Mock
    private CatalogService catalogService;

    private MetaStorageManager metaStorageManager;

    private final SimpleInMemoryKeyValueStorage metaStorageKvStorage = new SimpleInMemoryKeyValueStorage("test");

    private SchemaManager schemaManager;

    private ArgumentCaptor<EventListener<CatalogEventParameters>> tableCreatedListener;
    private ArgumentCaptor<EventListener<CatalogEventParameters>> tableAlteredListener;
    private ArgumentCaptor<EventListener<CatalogEventParameters>> tableDestroyedListener;

    @BeforeEach
    void setUp() {
        metaStorageManager = spy(StandaloneMetaStorageManager.create(metaStorageKvStorage));
        assertThat(metaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        tableCreatedListener = ArgumentCaptor.forClass(EventListener.class);
        tableAlteredListener = ArgumentCaptor.forClass(EventListener.class);
        tableDestroyedListener = ArgumentCaptor.forClass(EventListener.class);

        doNothing().when(catalogService).listen(eq(CatalogEvent.TABLE_CREATE), tableCreatedListener.capture());
        doNothing().when(catalogService).listen(eq(CatalogEvent.TABLE_ALTER), tableAlteredListener.capture());

        schemaManager = new SchemaManager(registry, catalogService);
        assertThat(schemaManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertThat("Watches were not deployed", metaStorageManager.deployWatches(), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        assertThat(stopAsync(new ComponentContext(), schemaManager, metaStorageManager), willCompleteSuccessfully());
    }

    private void createSomeTable() {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("k1", ColumnType.INT16, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("k2", ColumnType.STRING, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("v1", ColumnType.INT32, false, 0, 0, 0, null)
        );
        CatalogTableDescriptor tableDescriptor = new CatalogTableDescriptor(
                TABLE_ID, -1, -1, TABLE_NAME, 0, columns, List.of("k1", "k2"), null, DEFAULT_STORAGE_PROFILE
        );

        CompletableFuture<Boolean> future = tableCreatedListener()
                .notify(new CreateTableEventParameters(CAUSALITY_TOKEN_1, CATALOG_VERSION_1, tableDescriptor));

        assertThat(future, willBe(false));

        completeCausalityToken(CAUSALITY_TOKEN_1);
    }

    private EventListener<CatalogEventParameters> tableCreatedListener() {
        return Objects.requireNonNull(tableCreatedListener.getValue(), "tableCreatedListener is not registered with CatalogService");
    }

    private EventListener<CatalogEventParameters> tableAlteredListener() {
        return Objects.requireNonNull(tableAlteredListener.getValue(), "tableAlteredListener is not registered with CatalogService");
    }

    private EventListener<CatalogEventParameters> tableDestroyedListener() {
        return Objects.requireNonNull(tableDestroyedListener.getValue(), "tableDestroyedListener is not registered with CatalogService");
    }

    private static CatalogTableDescriptor tableDescriptorAfterColumnAddition() {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("k1", ColumnType.INT16, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("k2", ColumnType.STRING, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("v1", ColumnType.INT32, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("v2", ColumnType.STRING, false, 0, 0, 0, null)
        );

        return new CatalogTableDescriptor(
                TABLE_ID,
                -1,
                -1,
                TABLE_NAME,
                0,
                columns,
                List.of("k1", "k2"),
                null,
                DEFAULT_STORAGE_PROFILE
        ).newDescriptor(
                TABLE_NAME,
                INITIAL_TABLE_VERSION + 1,
                columns,
                INITIAL_CAUSALITY_TOKEN,
                DEFAULT_STORAGE_PROFILE
        );
    }

    private void completeCausalityToken(long causalityToken) {
        assertThat(onMetastoreRevisionCompleteHolder.get().apply(causalityToken), willCompleteSuccessfully());
    }

    @Test
    void latestSchemaRegistryIsUnavailableUntilSomeSchemaVersionIsProcessed() {
        assertThat(schemaManager.schemaRegistry(TABLE_ID), is(nullValue()));
    }

    @Test
    void latestSchemaRegistryIsAvailable() {
        createSomeTable();

        SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(TABLE_ID);

        assertThat(schemaRegistry.lastKnownSchemaVersion(), is(1));
        assertThat(schemaRegistry.schema(1).version(), is(1));
    }

    @Test
    void schemaRegistryByCausalityTokenIsUnavailableTillTokenIsCompleted() {
        createSomeTable();

        CompletableFuture<SchemaRegistry> future = schemaManager.schemaRegistry(CAUSALITY_TOKEN_2, TABLE_ID);

        assertThat(future, willTimeoutFast());
    }

    @Test
    void schemaRegistryByCausalityTokenIsAvailable() {
        createSomeTable();

        CompletableFuture<SchemaRegistry> future = schemaManager.schemaRegistry(CAUSALITY_TOKEN_1, TABLE_ID);
        assertThat(future, willCompleteSuccessfully());

        SchemaRegistry schemaRegistry = future.join();

        assertThat(schemaRegistry.lastKnownSchemaVersion(), is(1));
        assertThat(schemaRegistry.schema(1).version(), is(1));
    }

    @Test
    void previousSchemaVersionsRemainAvailable() {
        create2TableVersions();

        CompletableFuture<SchemaRegistry> future = schemaManager.schemaRegistry(CAUSALITY_TOKEN_2, TABLE_ID);
        assertThat(future, willCompleteSuccessfully());

        SchemaRegistry schemaRegistry = future.join();

        SchemaDescriptor schemaDescriptor1 = schemaRegistry.schema(1);
        assertThat(schemaDescriptor1.version(), is(1));

        SchemaDescriptor schemaDescriptor2 = schemaRegistry.schema(2);
        assertThat(schemaDescriptor2.version(), is(2));
    }

    private void create2TableVersions() {
        createSomeTable();
        addSomeColumn();
    }

    private void addSomeColumn() {
        when(catalogService.table(TABLE_ID, CATALOG_VERSION_2)).thenReturn(tableDescriptorAfterColumnAddition());

        AddColumnEventParameters event = new AddColumnEventParameters(
                CAUSALITY_TOKEN_2,
                CATALOG_VERSION_2,
                TABLE_ID,
                List.of(new CatalogTableColumnDescriptor("v2", ColumnType.STRING, false, 0, 0, 0, null))
        );

        CompletableFuture<Boolean> future = tableAlteredListener().notify(event);

        assertThat(future, willBe(false));

        completeCausalityToken(CAUSALITY_TOKEN_2);
    }

    @Test
    void destroyTableMakesRegistryUnavailable() {
        createSomeTable();

        assertThat(schemaManager.dropRegistryAsync(TABLE_ID), willCompleteSuccessfully());

        completeCausalityToken(CAUSALITY_TOKEN_2);

        CompletableFuture<SchemaRegistry> future = schemaManager.schemaRegistry(CAUSALITY_TOKEN_2, TABLE_ID);
        assertThat(future, is(completedFuture()));
        assertThat(future, willBe(nullValue()));
    }

    @Test
    void loadingPreExistingSchemasWorks() {
        create2TableVersions();

        assertThat(schemaManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        when(catalogService.latestCatalogVersion()).thenReturn(2);
        when(catalogService.tables(anyInt())).thenReturn(List.of(tableDescriptorAfterColumnAddition()));
        doReturn(CompletableFuture.completedFuture(CAUSALITY_TOKEN_2)).when(metaStorageManager).recoveryFinishedFuture();

        schemaManager = new SchemaManager(registry, catalogService);
        assertThat(schemaManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        completeCausalityToken(CAUSALITY_TOKEN_2);

        SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(TABLE_ID);

        int prevSchemaVersionNotYetTouched = 1;

        SchemaDescriptor schemaDescriptor = schemaRegistry.schema(prevSchemaVersionNotYetTouched);
        assertThat(schemaDescriptor.version(), is(prevSchemaVersionNotYetTouched));
    }
}
