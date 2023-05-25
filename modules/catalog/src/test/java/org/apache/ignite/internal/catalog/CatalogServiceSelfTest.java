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

package org.apache.ignite.internal.catalog;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.commands.altercolumn.AlterColumnAction;
import org.apache.ignite.internal.catalog.commands.altercolumn.AlterColumnDefault;
import org.apache.ignite.internal.catalog.commands.altercolumn.AlterColumnNotNull;
import org.apache.ignite.internal.catalog.commands.altercolumn.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.altercolumn.AlterColumnType;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * Catalog service self test.
 */
public class CatalogServiceSelfTest {
    private static final String TABLE_NAME = "myTable";
    private static final String TABLE_NAME_2 = "myTable2";

    private MetaStorageManager metastore;

    private VaultManager vault;

    private CatalogServiceImpl service;

    @BeforeEach
    void setUp() throws NodeStoppingException {
        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(
                vault, new SimpleInMemoryKeyValueStorage("test")
        );

        service = new CatalogServiceImpl(new UpdateLogImpl(metastore, vault));

        vault.start();
        metastore.start();
        service.start();

        metastore.deployWatches();
    }

    @AfterEach
    public void tearDown() throws Exception {
        service.stop();
        metastore.stop();
        vault.stop();
    }

    @Test
    public void testEmptyCatalog() {
        assertNotNull(service.activeSchema(System.currentTimeMillis()));
        assertNotNull(service.schema(0));

        assertNull(service.schema(1));
        assertThrows(IllegalStateException.class, () -> service.activeSchema(-1L));

        assertNull(service.table(0, System.currentTimeMillis()));
        assertNull(service.index(0, System.currentTimeMillis()));

        SchemaDescriptor schema = service.schema(0);
        assertEquals(CatalogService.PUBLIC, schema.name());

        assertEquals(0, schema.id());
        assertEquals(0, schema.version());
        assertEquals(0, schema.tables().length);
        assertEquals(0, schema.indexes().length);
    }

    @Test
    public void testCreateTable() {
        CreateTableParams params = CreateTableParams.builder()
                .schemaName("PUBLIC")
                .tableName(TABLE_NAME)
                .ifTableExists(true)
                .zone("ZONE")
                .columns(List.of(
                        new ColumnParams("key1", ColumnType.INT32, DefaultValue.constant(null), false),
                        new ColumnParams("key2", ColumnType.INT32, DefaultValue.constant(null), false),
                        new ColumnParams("val", ColumnType.INT32, DefaultValue.constant(null), true)
                ))
                .primaryKeyColumns(List.of("key1", "key2"))
                .colocationColumns(List.of("key2"))
                .build();

        CompletableFuture<Void> fut = service.createTable(params);

        assertThat(fut, willBe((Object) null));

        // Validate catalog version from the past.
        SchemaDescriptor schema = service.schema(0);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(CatalogService.PUBLIC, schema.name());
        assertEquals(0, schema.version());
        assertSame(schema, service.activeSchema(0L));
        assertSame(schema, service.activeSchema(123L));

        assertNull(schema.table(TABLE_NAME));
        assertNull(service.table(TABLE_NAME, 123L));
        assertNull(service.table(1, 123L));

        // Validate actual catalog
        schema = service.schema(1);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(CatalogService.PUBLIC, schema.name());
        assertEquals(1, schema.version());
        assertSame(schema, service.activeSchema(System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME), service.table(TABLE_NAME, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME), service.table(1, System.currentTimeMillis()));

        // Validate newly created table
        TableDescriptor table = schema.table(TABLE_NAME);

        assertEquals(1L, table.id());
        assertEquals(TABLE_NAME, table.name());
        assertEquals(0L, table.engineId());
        assertEquals(0L, table.zoneId());

        // Validate another table creation.
        fut = service.createTable(simpleTable(TABLE_NAME_2));

        assertThat(fut, willBe((Object) null));

        // Validate actual catalog has both tables.
        schema = service.schema(2);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(CatalogService.PUBLIC, schema.name());
        assertEquals(2, schema.version());
        assertSame(schema, service.activeSchema(System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME), service.table(TABLE_NAME, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME), service.table(1, System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME_2), service.table(TABLE_NAME_2, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME_2), service.table(2, System.currentTimeMillis()));

        assertNotSame(schema.table(TABLE_NAME), schema.table(TABLE_NAME_2));
    }

    @Test
    public void testCreateTableIfExistsFlag() {
        CreateTableParams params = CreateTableParams.builder()
                .tableName(TABLE_NAME)
                .columns(List.of(
                        new ColumnParams("key", ColumnType.INT32, DefaultValue.constant(null), false),
                        new ColumnParams("val", ColumnType.INT32, DefaultValue.constant(null), false)
                ))
                .primaryKeyColumns(List.of("key"))
                .ifTableExists(true)
                .build();

        assertThat(service.createTable(params), willBe((Object) null));
        assertThat(service.createTable(params), willThrowFast(TableAlreadyExistsException.class));

        CompletableFuture<?> fut = service.createTable(
                CreateTableParams.builder()
                        .tableName(TABLE_NAME)
                        .columns(List.of(
                                new ColumnParams("key", ColumnType.INT32, DefaultValue.constant(null), false),
                                new ColumnParams("val", ColumnType.INT32, DefaultValue.constant(null), false)
                        ))
                        .primaryKeyColumns(List.of("key"))
                        .ifTableExists(false)
                        .build());

        assertThat(fut, willThrowFast(TableAlreadyExistsException.class));
    }

    @Test
    public void testDropTable() throws InterruptedException {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));
        assertThat(service.createTable(simpleTable(TABLE_NAME_2)), willBe((Object) null));

        long beforeDropTimestamp = System.currentTimeMillis();

        DropTableParams dropTableParams = DropTableParams.builder().schemaName("PUBLIC").tableName(TABLE_NAME).build();

        // Timer must change.
        IgniteTestUtils.waitForCondition(() -> System.currentTimeMillis() != beforeDropTimestamp, 1, 1);

        assertThat(service.dropTable(dropTableParams), willBe((Object) null));

        // Validate catalog version from the past.
        SchemaDescriptor schema = service.schema(2);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(CatalogService.PUBLIC, schema.name());
        assertEquals(2, schema.version());
        assertSame(schema, service.activeSchema(beforeDropTimestamp));

        assertSame(schema.table(TABLE_NAME), service.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(schema.table(TABLE_NAME), service.table(1, beforeDropTimestamp));

        assertSame(schema.table(TABLE_NAME_2), service.table(TABLE_NAME_2, beforeDropTimestamp));
        assertSame(schema.table(TABLE_NAME_2), service.table(2, beforeDropTimestamp));

        // Validate actual catalog
        schema = service.schema(3);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(CatalogService.PUBLIC, schema.name());
        assertEquals(3, schema.version());
        assertSame(schema, service.activeSchema(System.currentTimeMillis()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(service.table(TABLE_NAME, System.currentTimeMillis()));
        assertNull(service.table(1, System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME_2), service.table(TABLE_NAME_2, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME_2), service.table(2, System.currentTimeMillis()));
    }

    @Test
    public void testDropTableIfExistsFlag() {
        CreateTableParams createTableParams = CreateTableParams.builder()
                .tableName(TABLE_NAME)
                .columns(List.of(
                        new ColumnParams("key", ColumnType.INT32, DefaultValue.constant(null), false),
                        new ColumnParams("val", ColumnType.INT32, DefaultValue.constant(null), false)
                ))
                .primaryKeyColumns(List.of("key"))
                .build();

        assertThat(service.createTable(createTableParams), willBe((Object) null));

        DropTableParams params = DropTableParams.builder()
                .tableName(TABLE_NAME)
                .ifTableExists(true)
                .build();

        assertThat(service.dropTable(params), willBe((Object) null));
        assertThat(service.dropTable(params), willThrowFast(TableNotFoundException.class));

        params = DropTableParams.builder()
                .tableName(TABLE_NAME)
                .ifTableExists(false)
                .build();

        assertThat(service.dropTable(params), willThrowFast(TableNotFoundException.class));
    }

    /**
     * Checks for possible changes to the default value of a column descriptor.
     *
     * <p>Set/drop default value allowed for any column.
     */
    @Test
    public void testAlterColumnDefault() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // NULL-> NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", new AlterColumnDefault((t) -> DefaultValue.constant(null))),
                willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // NULL -> 1 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", new AlterColumnDefault((t) -> DefaultValue.constant(1))),
                willBe((Object) null));
        assertNotNull(service.schema(++schemaVer));

        // 1 -> 1 : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", new AlterColumnDefault((t) -> DefaultValue.constant(1))),
                willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // 1 -> 2 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", new AlterColumnDefault((t) -> DefaultValue.constant(2))),
                willBe((Object) null));
        assertNotNull(service.schema(++schemaVer));

        // 2 -> NULL : Ok (for nullable column).
        assertThat(changeColumn(TABLE_NAME, "VAL", new AlterColumnDefault((t) -> DefaultValue.constant(null))),
                willBe((Object) null));
        assertNotNull(service.schema(++schemaVer));

        // NULL -> funcCall : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", new AlterColumnDefault((t) -> DefaultValue.functionCall("funcCall"))),
                willBe((Object) null));
        assertNotNull(service.schema(++schemaVer));

        // funcCall -> funcCall : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", new AlterColumnDefault((t) -> DefaultValue.functionCall("funcCall"))),
                willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the nullable flag of a column descriptor.
     *
     * <ul>
     *  <li>DROP NOT NULL is allowed on any column.
     *  <li>SET NOT NULL is forbidden.
     * </ul>
     */
    @Test
    public void testAlterColumnNotNull() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // NULLABLE -> NULLABLE : No-op.
        // NOT NULL -> NOT NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", new AlterColumnNotNull(false)), willBe((Object) null));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", new AlterColumnNotNull(true)), willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // NOT NULL -> NULlABLE : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", new AlterColumnNotNull(false)), willBe((Object) null));
        assertNotNull(service.schema(++schemaVer));

        // NULlABLE -> NOT NULL : Error.
        assertThat(changeColumn(TABLE_NAME, "VAL", new AlterColumnNotNull(true)),
                willThrowFast(SqlException.class, "Cannot set NOT NULL for column 'VAL'"));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", new AlterColumnNotNull(true)),
                willThrowFast(SqlException.class, "Cannot set NOT NULL for column 'VAL_NOT_NULL'"));
        assertNull(service.schema(schemaVer + 1));
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "DECIMAL", "STRING", "BYTE_ARRAY"}, mode = Mode.EXCLUDE)
    public void testAlterColumnTypeAnyPrecisionChangeIsRejected(ColumnType type) {
        ColumnParams col = ColumnParams.builder().name("COL").type(type).build();
        ColumnParams colWithPrecision = ColumnParams.builder().name("COL_PRECISION").type(type).precision(10).build();

        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(col, colWithPrecision))), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        assertThat(
                changeColumn(TABLE_NAME, col.name(), new AlterColumnType(type, 10, null)),
                willThrowFast(SqlException.class, "Cannot change precision for column '" + col.name() + "'")
        );

        assertThat(
                changeColumn(TABLE_NAME, colWithPrecision.name(), new AlterColumnType(type, 10, null)),
                willThrowFast(SqlException.class, "Cannot change precision for column '" + colWithPrecision.name() + "'")
        );

        assertNull(service.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the precision of a column descriptor.
     *
     * <ul>
     *  <li>Decreasing precision (and length for varlen types) is forbidden.</li>
     *  <li>Increasing precision is allowed for {@link ColumnType#DECIMAL}.</li>
     *  <li>Increasing length is allowed for {@link ColumnType#STRING} and {@link ColumnType#BYTE_ARRAY}.</li>
     * </ul>
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"DECIMAL", "STRING", "BYTE_ARRAY"}, mode = Mode.INCLUDE)
    public void testAlterColumnTypePrecision(ColumnType type) {
        ColumnParams col = new ColumnParams("COL_" + type, type, DefaultValue.constant(null), false);
        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(col))), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // ANY-> UNDEFINED PRECISION : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new AlterColumnType(col.type(), null, null)), willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // UNDEFINED PRECISION -> 10 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new AlterColumnType(col.type(), 10, null)),
                willBe((Object) null)
        );
        assertNotNull(service.schema(++schemaVer));

        // 10 -> 11 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new AlterColumnType(col.type(), 11, null)),
                willBe((Object) null)
        );

        SchemaDescriptor schema = service.schema(++schemaVer);
        assertNotNull(schema);

        TableColumnDescriptor desc = schema.table(TABLE_NAME).column(col.name());

        assertNotSame(desc.length(), desc.precision());
        assertEquals(11, col.type() == ColumnType.DECIMAL ? desc.precision() : desc.length());

        // 11 -> 10 : Error.
        String expMsg = col.type() == ColumnType.DECIMAL
                ? "Cannot decrease precision for column '" + col.name() + "' [from=11, to=10]."
                : "Cannot decrease length for column '" + col.name() + "' [from=11, to=10].";

        assertThat(
                changeColumn(TABLE_NAME, col.name(), new AlterColumnType(col.type(), 10, null)),
                willThrowFast(SqlException.class, expMsg)
        );
        assertNull(service.schema(schemaVer + 1));
    }

    /** Ensures that the scale cannot be changed for any type. */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testAlterColumnTypeScale(ColumnType type) {
        ColumnParams col = ColumnParams.builder().name("COL_" + type).type(type).scale(3).build();
        assertThat(service.createTable(simpleTable(TABLE_NAME, List.of(col))), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        // ANY-> UNDEFINED SCALE : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new AlterColumnType(col.type(), null, null)), willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // 3 -> 3 : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new AlterColumnType(col.type(), null, 3)),
                willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));

        // 3 -> 4 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new AlterColumnType(col.type(), null, 4)),
                willThrowFast(SqlException.class, "Cannot change scale for column '" + col.name() + "' [from=3, to=4]."));
        assertNull(service.schema(schemaVer + 1));

        // 3 -> 2 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new AlterColumnType(col.type(), null, 2)),
                willThrowFast(SqlException.class, "Cannot change scale for column '" + col.name() + "' [from=3, to=2]."));
        assertNull(service.schema(schemaVer + 1));
    }


    /**
     * Checks for possible changes of the type of a column descriptor.
     *
     * <p>The following transitions are allowed:
     * <ul>
     *     <li>INT8 -> INT16 -> INT32 -> INT64</li>
     *     <li>FLOAT -> DOUBLE</li>
     * </ul>
     * All other transitions must be rejected.
     */
    @ParameterizedTest(name = "set data type {0}")
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testAlterColumnType(ColumnType target) {
        EnumSet<ColumnType> types = EnumSet.allOf(ColumnType.class);
        types.remove(ColumnType.NULL);

        List<ColumnParams> columns = types.stream()
                .map(t -> new ColumnParams("COL_" + t, t, DefaultValue.constant(null), false))
                .collect(Collectors.toList());

        CreateTableParams createTableParams = simpleTable(TABLE_NAME, columns);

        Map<ColumnType, Set<ColumnType>> validTransitions = new EnumMap<>(ColumnType.class);
        validTransitions.put(ColumnType.INT8, EnumSet.of(ColumnType.INT16, ColumnType.INT32, ColumnType.INT64));
        validTransitions.put(ColumnType.INT16, EnumSet.of(ColumnType.INT32, ColumnType.INT64));
        validTransitions.put(ColumnType.INT32, EnumSet.of(ColumnType.INT64));
        validTransitions.put(ColumnType.FLOAT, EnumSet.of(ColumnType.DOUBLE));

        assertThat(service.createTable(createTableParams), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        AlterColumnType changeType = new AlterColumnType(target, null, null);

        for (ColumnParams col : columns) {
            TypeSafeMatcher<CompletableFuture<?>> matcher;
            boolean sameType = col.type() == target;

            if (sameType || (validTransitions.containsKey(col.type()) && validTransitions.get(col.type()).contains(target))) {
                matcher = willBe((Object) null);
                schemaVer += sameType ? 0 : 1;
            } else {
                matcher = willThrowFast(SqlException.class,
                        "Cannot change data type for column '" + col.name() + "' [from=" + col.type() + ", to=" + target + "].");
            }

            assertThat(col.type() + " -> " + target, changeColumn(TABLE_NAME, col.name(), changeType), matcher);
            assertNotNull(service.schema(schemaVer));
            assertNull(service.schema(schemaVer + 1));
        }
    }

    @Test
    public void testAlterColumnMultipleChanges() {
        assertThat(service.createTable(simpleTable(TABLE_NAME)), willBe((Object) null));

        int schemaVer = 1;
        assertNotNull(service.schema(schemaVer));
        assertNull(service.schema(schemaVer + 1));

        AlterColumnAction[] actions = {
                new AlterColumnDefault(t -> DefaultValue.constant(null)),
                new AlterColumnNotNull(false),
                new AlterColumnType(ColumnType.INT64, null, null)
        };

        // Ensures that 3 different actions applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", actions), willBe((Object) null));

        SchemaDescriptor schema = service.schema(++schemaVer);
        assertNotNull(schema);

        TableColumnDescriptor desc = schema.table(TABLE_NAME).column("VAL_NOT_NULL");
        assertEquals(DefaultValue.constant(null), desc.defaultValue());
        assertTrue(desc.nullable());
        assertEquals(ColumnType.INT64, desc.type());

        // Ensures that only one of three actions applied.
        actions[0] = new AlterColumnDefault(t -> DefaultValue.constant(2));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", actions), willBe((Object) null));

        schema = service.schema(++schemaVer);
        assertNotNull(schema);
        assertEquals(DefaultValue.constant(2), schema.table(TABLE_NAME).column("VAL_NOT_NULL").defaultValue());

        // Ensures that no action will be applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", actions), willBe((Object) null));
        assertNull(service.schema(schemaVer + 1));
    }

    @Test
    public void operationWillBeRetriedFiniteAmountOfTimes() {
        UpdateLog updateLogMock = Mockito.mock(UpdateLog.class);

        ArgumentCaptor<OnUpdateHandler> updateHandlerCapture = ArgumentCaptor.forClass(OnUpdateHandler.class);

        doNothing().when(updateLogMock).registerUpdateHandler(updateHandlerCapture.capture());

        CatalogServiceImpl service = new CatalogServiceImpl(updateLogMock);
        service.start();

        when(updateLogMock.append(any())).thenAnswer(invocation -> {
            // here we emulate concurrent updates. First of all, we return a future completed with "false"
            // as if someone has concurrently appended an update. Besides, in order to unblock service and allow to
            // make another attempt, we must notify service with the same version as in current attempt.
            VersionedUpdate updateFromInvocation = invocation.getArgument(0, VersionedUpdate.class);

            VersionedUpdate update = new VersionedUpdate(
                    updateFromInvocation.version(),
                    List.of(new ObjectIdGenUpdateEntry(1))
            );

            updateHandlerCapture.getValue().handle(update);

            return completedFuture(false);
        });

        CompletableFuture<Void> createTableFut = service.createTable(simpleTable("T"));

        assertThat(createTableFut, willThrow(IgniteInternalException.class, "Max retry limit exceeded"));

        // retry limit is hardcoded at org.apache.ignite.internal.catalog.CatalogServiceImpl.MAX_RETRY_COUNT
        Mockito.verify(updateLogMock, times(10)).append(any());
    }

    @Test
    public void catalogServiceManagesUpdateLogLifecycle() throws Exception {
        UpdateLog updateLogMock = Mockito.mock(UpdateLog.class);

        CatalogServiceImpl service = new CatalogServiceImpl(updateLogMock);

        service.start();

        verify(updateLogMock).start();

        service.stop();

        verify(updateLogMock).stop();
    }

    @Test
    public void testCreateTableEvents() {
        CreateTableParams params = CreateTableParams.builder()
                .schemaName("PUBLIC")
                .tableName(TABLE_NAME)
                .ifTableExists(true)
                .zone("ZONE")
                .columns(List.of(
                        new ColumnParams("key1", ColumnType.INT32, DefaultValue.constant(null), false),
                        new ColumnParams("key2", ColumnType.INT32, DefaultValue.constant(null), false),
                        new ColumnParams("val", ColumnType.INT32, DefaultValue.constant(null), true)
                ))
                .primaryKeyColumns(List.of("key1", "key2"))
                .colocationColumns(List.of("key2"))
                .build();

        EventListener<CatalogEventParameters> eventListener = Mockito.mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(completedFuture(false));

        service.listen(CatalogEvent.TABLE_CREATE, eventListener);
        service.listen(CatalogEvent.TABLE_DROP, eventListener);

        CompletableFuture<Void> fut = service.createTable(params);

        assertThat(fut, willBe((Object) null));

        verify(eventListener).notify(any(CreateTableEventParameters.class), ArgumentMatchers.isNull());

        DropTableParams dropTableparams = DropTableParams.builder().tableName(TABLE_NAME).build();

        fut = service.dropTable(dropTableparams);

        assertThat(fut, willBe((Object) null));

        verify(eventListener).notify(any(DropTableEventParameters.class), ArgumentMatchers.isNull());
        verifyNoMoreInteractions(eventListener);
    }

    private CompletableFuture<Void> changeColumn(String tab, String col, AlterColumnAction... change) {
        return service.alterColumn(AlterColumnParams.builder()
                .tableName(tab)
                .columnName(col)
                .changeActions(List.of(change))
                .build());
    }

    private static CreateTableParams simpleTable(String name) {
        List<ColumnParams> cols = List.of(
                new ColumnParams("ID", ColumnType.INT32, DefaultValue.constant(null), false),
                new ColumnParams("VAL", ColumnType.INT32, DefaultValue.constant(null), true),
                new ColumnParams("VAL_NOT_NULL", ColumnType.INT32, DefaultValue.constant(1), false),
                new ColumnParams("DEC", ColumnType.DECIMAL, DefaultValue.constant(null), true),
                new ColumnParams("STR", ColumnType.STRING, DefaultValue.constant(null), true),
                ColumnParams.builder().name("DEC_SCALE").type(ColumnType.DECIMAL).scale(3).build()
        );

        return simpleTable(name, cols);
    }

    private static CreateTableParams simpleTable(String name, List<ColumnParams> cols) {
        return CreateTableParams.builder()
                .schemaName("PUBLIC")
                .tableName(name)
                .zone("ZONE")
                .columns(cols)
                .primaryKeyColumns(List.of(cols.get(0).name()))
                .build();
    }
}
