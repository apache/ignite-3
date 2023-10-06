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


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.AlterColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.subscription.ListAccumulator;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CatalogSchemaManagerTest extends BaseIgniteAbstractTest {
    private static final String SCHEMA_STORE_PREFIX = ".sch-hist.";
    private static final String LATEST_SCHEMA_VERSION_STORE_SUFFIX = ".sch-hist-latest";

    private static final int TABLE_ID = 3;
    private static final String TABLE_NAME = "t";

    private static final long CAUSALITY_TOKEN_1 = 42;
    private static final long CAUSALITY_TOKEN_2 = 45;

    private static final int CATALOG_VERSION_1 = 10;
    private static final int CATALOG_VERSION_2 = 11;

    private final AtomicReference<LongFunction<CompletableFuture<?>>> onMetastoreRevisionCompleteHolder = new AtomicReference<>();

    private final Consumer<LongFunction<CompletableFuture<?>>> registry = onMetastoreRevisionCompleteHolder::set;

    @Mock
    private CatalogService catalogService;

    private VaultManager vaultManager;

    private MetaStorageManager metaStorageManager;

    private final SimpleInMemoryKeyValueStorage metaStorageKvStorage = new SimpleInMemoryKeyValueStorage("test");

    private CatalogSchemaManager schemaManager;

    private EventListener<CatalogEventParameters> tableCreatedListener;
    private EventListener<CatalogEventParameters> tableAlteredListener;

    private final Exception cause = new Exception("Oops");

    @BeforeEach
    void setUp() {
        vaultManager = new VaultManager(new InMemoryVaultService());
        vaultManager.start();

        metaStorageManager = spy(StandaloneMetaStorageManager.create(vaultManager, metaStorageKvStorage));
        metaStorageManager.start();

        doAnswer(invocation -> {
            tableCreatedListener = invocation.getArgument(1);
            return null;
        }).when(catalogService).listen(eq(CatalogEvent.TABLE_CREATE), any());

        doAnswer(invocation -> {
            tableAlteredListener = invocation.getArgument(1);
            return null;
        }).when(catalogService).listen(eq(CatalogEvent.TABLE_ALTER), any());

        schemaManager = new CatalogSchemaManager(registry, catalogService, metaStorageManager);
        schemaManager.start();

        assertThat("Watches were not deployed", metaStorageManager.deployWatches(), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() throws Exception {
        schemaManager.stop();
        metaStorageManager.stop();
        vaultManager.stop();
    }

    @Test
    void savesSchemaOnTableCreation() {
        createSomeTable();

        SchemaDescriptor schemaDescriptor = getSchemaDescriptor(1);

        assertThat(schemaDescriptor.version(), is(1));
        assertThat(schemaDescriptor.columnNames(), contains("k1", "k2", "v1"));

        Column k1 = schemaDescriptor.column("k1");
        assertThat(k1, is(notNullValue()));

        assertThat(k1.name(), is("k1"));
        assertThat(k1.type().spec(), is(NativeTypeSpec.INT16));

        Column v1 = schemaDescriptor.column("v1");
        assertThat(v1, is(notNullValue()));

        assertThat(v1.name(), is("v1"));
        assertThat(v1.type().spec(), is(NativeTypeSpec.INT32));
    }

    private void createSomeTable() {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("k1", ColumnType.INT16, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("k2", ColumnType.STRING, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("v1", ColumnType.INT32, false, 0, 0, 0, null)
        );
        CatalogTableDescriptor tableDescriptor = new CatalogTableDescriptor(
                TABLE_ID, -1, TABLE_NAME, 0, 1, columns, List.of("k1", "k2"), null, INITIAL_CAUSALITY_TOKEN
        );

        CompletableFuture<Boolean> future = tableCreatedListener()
                .notify(new CreateTableEventParameters(CAUSALITY_TOKEN_1, CATALOG_VERSION_1, tableDescriptor), null);

        assertThat(future, willBe(false));

        completeCausalityToken(CAUSALITY_TOKEN_1);
    }

    private EventListener<CatalogEventParameters> tableCreatedListener() {
        return Objects.requireNonNull(tableCreatedListener, "tableCreatedListener is not registered with CatalogService");
    }

    private EventListener<CatalogEventParameters> tableAlteredListener() {
        return Objects.requireNonNull(tableAlteredListener, "tableAlteredListener is not registered with CatalogService");
    }

    private SchemaDescriptor getSchemaDescriptor(int schemaVersion) {
        Entry entry = metaStorageKvStorage.get(schemaWithVerHistKey(TABLE_ID, schemaVersion).bytes());
        assertThat(entry, is(notNullValue()));

        byte[] value = entry.value();
        assertThat(value, is(notNullValue()));

        return SchemaSerializerImpl.INSTANCE.deserialize(value);
    }

    private static ByteArray schemaWithVerHistKey(int tblId, int ver) {
        return ByteArray.fromString(tblId + SCHEMA_STORE_PREFIX + ver);
    }

    @Test
    void savesSchemaOnColumnAddition() {
        createSomeTable();

        when(catalogService.table(TABLE_ID, CATALOG_VERSION_2)).thenReturn(tableDescriptorAfterColumnAddition());

        AddColumnEventParameters event = new AddColumnEventParameters(
                CAUSALITY_TOKEN_2,
                CATALOG_VERSION_2,
                TABLE_ID,
                List.of(new CatalogTableColumnDescriptor("v2", ColumnType.STRING, false, 0, 0, 0, null))
        );

        CompletableFuture<Boolean> future = tableAlteredListener().notify(event, null);

        assertThat(future, willBe(false));

        SchemaDescriptor schemaDescriptor = getSchemaDescriptor(2);

        assertThat(schemaDescriptor.version(), is(2));
        assertThat(schemaDescriptor.columnNames(), contains("k1", "k2", "v1", "v2"));

        Column v2 = schemaDescriptor.column("v2");
        assertThat(v2, is(notNullValue()));

        assertThat(v2.name(), is("v2"));
        assertThat(v2.type().spec(), is(NativeTypeSpec.STRING));
    }

    private static CatalogTableDescriptor tableDescriptorAfterColumnAddition() {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("k1", ColumnType.INT16, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("k2", ColumnType.STRING, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("v1", ColumnType.INT32, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("v2", ColumnType.STRING, false, 0, 0, 0, null)
        );

        return new CatalogTableDescriptor(TABLE_ID, -1, TABLE_NAME, 0, 2, columns, List.of("k1", "k2"), null, INITIAL_CAUSALITY_TOKEN);
    }

    private void completeCausalityToken(long causalityToken) {
        assertThat(onMetastoreRevisionCompleteHolder.get().apply(causalityToken), willCompleteSuccessfully());
    }

    @Test
    void savesSchemaOnColumnRemoval() {
        createSomeTable();

        when(catalogService.table(TABLE_ID, CATALOG_VERSION_2)).thenReturn(tableDescriptorAfterColumnRemoval());

        DropColumnEventParameters event = new DropColumnEventParameters(
                CAUSALITY_TOKEN_2,
                CATALOG_VERSION_2,
                TABLE_ID,
                List.of("v1")
        );

        CompletableFuture<Boolean> future = tableAlteredListener().notify(event, null);

        assertThat(future, willBe(false));

        SchemaDescriptor schemaDescriptor = getSchemaDescriptor(2);

        assertThat(schemaDescriptor.version(), is(2));
        assertThat(schemaDescriptor.columnNames(), contains("k1", "k2"));
    }

    private static CatalogTableDescriptor tableDescriptorAfterColumnRemoval() {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("k1", ColumnType.INT16, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("k2", ColumnType.STRING, false, 0, 0, 0, null)
        );

        return new CatalogTableDescriptor(TABLE_ID, -1, TABLE_NAME, 0, 2, columns, List.of("k1", "k2"), null, INITIAL_CAUSALITY_TOKEN);
    }

    @Test
    void savesSchemaOnColumnAlteration() {
        createSomeTable();

        when(catalogService.table(TABLE_ID, CATALOG_VERSION_2)).thenReturn(tableDescriptorAfterColumnAlteration());

        AlterColumnEventParameters event = new AlterColumnEventParameters(
                CAUSALITY_TOKEN_2,
                CATALOG_VERSION_2,
                TABLE_ID,
                new CatalogTableColumnDescriptor("v1", ColumnType.INT64, false, 0, 0, 0, null)
        );

        CompletableFuture<Boolean> future = tableAlteredListener().notify(event, null);

        assertThat(future, willBe(false));

        SchemaDescriptor schemaDescriptor = getSchemaDescriptor(2);

        assertThat(schemaDescriptor.version(), is(2));

        Column v1 = schemaDescriptor.column("v1");
        assertThat(v1, is(notNullValue()));

        assertThat(v1.name(), is("v1"));
        assertThat(v1.type().spec(), is(NativeTypeSpec.INT64));
    }

    private static CatalogTableDescriptor tableDescriptorAfterColumnAlteration() {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("k1", ColumnType.INT32, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("k2", ColumnType.STRING, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("v1", ColumnType.INT64, false, 0, 0, 0, null)
        );

        return new CatalogTableDescriptor(TABLE_ID, -1, TABLE_NAME, 0, 2, columns, List.of("k1", "k2"), null, INITIAL_CAUSALITY_TOKEN);
    }

    @Test
    void propagatesExceptionFromCatalogOnTableCreation() {
        CompletableFuture<Boolean> future = tableCreatedListener().notify(mock(CreateTableEventParameters.class), cause);

        assertThat(future, willThrow(equalTo(cause)));
    }

    @Test
    void propagatesExceptionFromCatalogOnColumnAddition() {
        CompletableFuture<Boolean> future = tableAlteredListener().notify(mock(AddColumnEventParameters.class), cause);

        assertThat(future, willThrow(equalTo(cause)));
    }

    @Test
    void propagatesExceptionFromCatalogOnColumnRemoval() {
        CompletableFuture<Boolean> future = tableAlteredListener().notify(mock(DropColumnEventParameters.class), cause);

        assertThat(future, willThrow(equalTo(cause)));
    }

    @Test
    void propagatesExceptionFromCatalogOnColumnAlteration() {
        CompletableFuture<Boolean> future = tableAlteredListener().notify(mock(AddColumnEventParameters.class), cause);

        assertThat(future, willThrow(equalTo(cause)));
    }

    @Test
    void latestSchemaRegistryIsUnavailableUntilSomeSchemaVersionIsProcessed() {
        assertThat(schemaManager.schemaRegistry(TABLE_ID), is(nullValue()));
    }

    @Test
    void latestSchemaRegistryIsAvailable() {
        createSomeTable();

        SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(TABLE_ID);

        assertThat(schemaRegistry.schema().version(), is(1));
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

        assertThat(schemaRegistry.schema().version(), is(1));
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

        CompletableFuture<Boolean> future = tableAlteredListener().notify(event, null);

        assertThat(future, willBe(false));

        completeCausalityToken(CAUSALITY_TOKEN_2);
    }

    @Test
    void waitLatestSchemaReturnsLatestSchema() {
        create2TableVersions();

        SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(TABLE_ID);

        SchemaDescriptor schemaDescriptor = schemaRegistry.waitLatestSchema();

        assertThat(schemaDescriptor.version(), is(2));
    }

    @Test
    void dropRegistryMakesItUnavailable() {
        createSomeTable();

        assertThat(schemaManager.dropRegistry(CAUSALITY_TOKEN_2, TABLE_ID), willCompleteSuccessfully());

        completeCausalityToken(CAUSALITY_TOKEN_2);

        CompletableFuture<SchemaRegistry> future = schemaManager.schemaRegistry(CAUSALITY_TOKEN_2, TABLE_ID);
        assertThat(future, is(completedFuture()));

        SchemaRegistry schemaRegistry = future.join();

        assertThat(schemaRegistry, is(nullValue()));
    }

    @Test
    void dropRegistryRemovesSchemasFromMetastorage() {
        createSomeTable();

        assertThat(schemaManager.dropRegistry(CAUSALITY_TOKEN_2, TABLE_ID), willCompleteSuccessfully());

        completeCausalityToken(CAUSALITY_TOKEN_2);

        assertThatNoSchemasExist(TABLE_ID);
        assertThatNoLatestSchemaVersionExists(TABLE_ID);
    }

    private void assertThatNoSchemasExist(int tableId) {
        CompletableFuture<List<String>> schemaEntryKeysFuture = new CompletableFuture<>();

        Publisher<Entry> publisher = metaStorageManager.prefix(ByteArray.fromString(tableId + SCHEMA_STORE_PREFIX));
        publisher.subscribe(
                new ListAccumulator<Entry, String>(entry -> new String(entry.key(), UTF_8))
                        .toSubscriber(schemaEntryKeysFuture)
        );

        assertThat(schemaEntryKeysFuture, willBe(empty()));
    }

    private void assertThatNoLatestSchemaVersionExists(int tableId) {
        CompletableFuture<Entry> future = metaStorageManager.get(latestSchemaVersionKey(tableId));

        assertThat(future, willCompleteSuccessfully());

        Entry entry = future.join();

        if (entry != null) {
            assertThat(entry.value(), is(nullValue()));
        }
    }

    private static ByteArray latestSchemaVersionKey(int tableId) {
        return ByteArray.fromString(tableId + LATEST_SCHEMA_VERSION_STORE_SUFFIX);
    }

    @Test
    void loadingPreExistingSchemasWorks() throws Exception {
        create2TableVersions();

        schemaManager.stop();

        when(catalogService.latestCatalogVersion()).thenReturn(2);
        when(catalogService.tables(anyInt())).thenReturn(List.of(tableDescriptorAfterColumnAddition()));
        doReturn(45L).when(metaStorageManager).appliedRevision();

        schemaManager = new CatalogSchemaManager(registry, catalogService, metaStorageManager);
        schemaManager.start();

        SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(TABLE_ID);

        int prevSchemaVersionNotYetTouched = 1;

        SchemaDescriptor schemaDescriptor = schemaRegistry.schema(prevSchemaVersionNotYetTouched);
        assertThat(schemaDescriptor.version(), is(prevSchemaVersionNotYetTouched));
    }
}
