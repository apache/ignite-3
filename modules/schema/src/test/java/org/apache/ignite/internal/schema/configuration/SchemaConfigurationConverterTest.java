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

package org.apache.ignite.internal.schema.configuration;

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.EntryCountBudgetConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfiguration;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.IndexValidator;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.configuration.schemas.table.TableIndexConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableValidator;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.schemas.table.UnlimitedBudgetConfigurationSchema;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.schema.definition.ColumnDefinitionImpl;
import org.apache.ignite.internal.schema.testutils.builder.HashIndexDefinitionBuilder;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.builder.SortedIndexDefinitionBuilder;
import org.apache.ignite.internal.schema.testutils.builder.TableDefinitionBuilder;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.DefaultValueDefinition;
import org.apache.ignite.schema.definition.DefaultValueDefinition.ConstantValue;
import org.apache.ignite.schema.definition.DefaultValueDefinition.FunctionCall;
import org.apache.ignite.schema.definition.DefaultValueGenerators;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.index.HashIndexDefinition;
import org.apache.ignite.schema.definition.index.IndexColumnDefinition;
import org.apache.ignite.schema.definition.index.SortOrder;
import org.apache.ignite.schema.definition.index.SortedIndexColumnDefinition;
import org.apache.ignite.schema.definition.index.SortedIndexDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * SchemaConfigurationConverter tests.
 */
@SuppressWarnings("InstanceVariableMayNotBeInitialized")
public class SchemaConfigurationConverterTest extends AbstractSchemaConverterTest {
    /** Table builder. */
    private TableDefinitionBuilder tblBuilder;

    /** Configuration registry with one table for each test. */
    private ConfigurationRegistry confRegistry;

    /** Registered table id. */
    private UUID tableId;

    /**
     * Prepare configuration registry for test.
     *
     * @throws ExecutionException If failed.
     * @throws InterruptedException If failed.
     */
    @BeforeEach
    public void createRegistry() throws Exception {
        confRegistry = new ConfigurationRegistry(
                List.of(TablesConfiguration.KEY),
                Map.of(TableValidator.class, Set.of(TableValidatorImpl.INSTANCE),
                        IndexValidator.class, Set.of(IndexValidatorImpl.INSTANCE)),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of(
                        HashIndexConfigurationSchema.class,
                        SortedIndexConfigurationSchema.class,

                        UnknownDataStorageConfigurationSchema.class,
                        ConstantValueDefaultConfigurationSchema.class,
                        FunctionCallDefaultConfigurationSchema.class,
                        NullValueDefaultConfigurationSchema.class,
                        UnlimitedBudgetConfigurationSchema.class,
                        EntryCountBudgetConfigurationSchema.class
                )
        );

        confRegistry.start();

        tblBuilder = SchemaBuilders.tableBuilder("SNAME", "TNAME")
                .columns(
                        SchemaBuilders.column("COL1", ColumnType.DOUBLE).build(),
                        SchemaBuilders.column("COL2", ColumnType.DOUBLE).build(),
                        SchemaBuilders.column("A", ColumnType.INT8).build(),
                        SchemaBuilders.column("B", ColumnType.INT8).build(),
                        SchemaBuilders.column("C", ColumnType.INT8).build()
                ).withPrimaryKey("COL1");

        TableDefinition tbl = tblBuilder.build();

        confRegistry.getConfiguration(TablesConfiguration.KEY).change(
                ch -> SchemaConfigurationConverter.createTable(tbl, ch)
                        .changeTables(
                                tblsCh -> tblsCh.update(tbl.canonicalName(), tblCh -> tblCh.changeReplicas(1).changeTableId(1))
                        )
        ).get();

        NamedConfigurationTree<TableConfiguration, TableView, TableChange> cfg0 = getConfiguration().tables();

        List<UUID> ids = ((NamedListConfiguration<TableConfiguration, ?, ?>) cfg0).internalIds();

        assertEquals(1, ids.size());

        tableId = ids.get(0);
    }

    @AfterEach
    void tearDown() throws Exception {
        confRegistry.stop();
    }

    /**
     * Add/remove HashIndex into configuration and read it back.
     */
    @Test
    public void testConvertHashIndex() throws Exception {
        HashIndexDefinitionBuilder builder = SchemaBuilders.hashIndex("testHI")
                .withColumns("A", "B", "C")
                .withHints(Collections.singletonMap("param", "value"));
        HashIndexDefinition idx = builder.build();

        getConfiguration().indexes().change(change -> change.create(idx.name(), ch ->
                SchemaConfigurationConverter.addIndex(idx, tableId, ch))).get();

        HashIndexConfiguration idxCfg = (HashIndexConfiguration) getConfiguration().indexes().get(idx.name());

        assertNotNull(idxCfg);

        assertEquals("HASH", idxCfg.type().value());
        assertEquals(3, idxCfg.columnNames().value().length);
        assertEquals(idxCfg.tableId().value(), tableId);
    }

    /**
     * Add/remove SortedIndex into configuration and read it back.
     */
    @Test
    public void testConvertSortedIndex() throws Exception {
        SortedIndexDefinitionBuilder builder = SchemaBuilders.sortedIndex("SIDX");

        builder.addIndexColumn("A").asc().done();
        builder.addIndexColumn("B").desc().done();

        SortedIndexDefinition idx = builder.build();

        NamedConfigurationTree<TableIndexConfiguration, TableIndexView, TableIndexChange> idxConfig = getConfiguration().indexes();

        idxConfig.change(change -> change.create(idx.name(), ch ->
                SchemaConfigurationConverter.addIndex(idx, tableId, ch))).get();;

        SortedIndexView idx2 = (SortedIndexView) idxConfig.get(idx.name()).value();

        assertNotNull(idx2);
        assertEquals("SORTED", idx2.type());
        assertEquals(2, idx2.columns().size());
        assertEquals("A", idx2.columns().get(0).name());
        assertEquals("B", idx2.columns().get(1).name());
        SortedIndexColumnDefinition col0 = SchemaConfigurationConverter.convert(idx2.columns().get(0));
        SortedIndexColumnDefinition col1 = SchemaConfigurationConverter.convert(idx2.columns().get(1));
        assertEquals(SortOrder.ASC, col0.sortOrder());
        assertEquals(SortOrder.DESC, col1.sortOrder());
    }

    /**
     * Add/remove index on primary key into configuration and read it back.
     */
    @Test
    public void testUniqIndex() throws Exception {
        SortedIndexDefinition idx = SchemaBuilders.sortedIndex("pk_sorted")
                .addIndexColumn("COL1").desc().done()
                .unique(true)
                .build();

        getConfiguration().indexes().change(change -> change.create(idx.name(), ch ->
                SchemaConfigurationConverter.addIndex(idx, tableId, ch))).get();

        SortedIndexView idx2 = (SortedIndexView) getConfiguration().indexes().get(idx.name()).value();

        assertNotNull(idx2);
        assertEquals("PK_SORTED", idx2.name());
        assertEquals("SORTED", idx2.type());
        assertEquals(idx.columns().stream().map(IndexColumnDefinition::name).collect(Collectors.toList()),
                new ArrayList<>(idx2.columns().namedListKeys()));
        assertTrue(idx2.uniq());
    }

    /**
     * Detect an index containing affinity key as unique one.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15483")
    @Test
    public void testUniqueIndexDetection() throws Exception {
        SortedIndexDefinition idx = SchemaBuilders.sortedIndex("uniq_sorted")
                .addIndexColumn("A").done()
                .addIndexColumn("COL1").desc().done()
                .build();

        getConfiguration().indexes().change(change -> change.create(idx.canonicalName(), ch ->
                SchemaConfigurationConverter.addIndex(idx, tableId, ch))).get();

        SortedIndexView idx2 = (SortedIndexView) getConfiguration().indexes().get(idx.name()).value();

        assertNotNull(idx2);
        assertEquals("uniq_sorted", idx2.name().toLowerCase(Locale.US));
        assertEquals("SORTED", idx2.type());

        assertTrue(idx2.uniq());

        assertEquals(2, idx2.columns().size());
        assertEquals("A", idx2.columns().get(0).name());
        assertEquals("COL1", idx2.columns().get(1).name());
        SortedIndexColumnDefinition col0 = SchemaConfigurationConverter.convert(idx2.columns().get(0));
        SortedIndexColumnDefinition col1 = SchemaConfigurationConverter.convert(idx2.columns().get(1));
        assertEquals(SortOrder.ASC, col0.sortOrder());
        assertEquals(SortOrder.DESC, col1.sortOrder());
    }

    /**
     * Add/remove table and read it back.
     */
    @Test
    public void testConvertTable() {
        TableDefinition tbl = tblBuilder.build();

        TableConfiguration tblCfg = confRegistry.getConfiguration(TablesConfiguration.KEY).tables().get(tbl.canonicalName());

        TableDefinition tbl2 = SchemaConfigurationConverter.convert(tblCfg);

        assertEquals(tbl.canonicalName(), tbl2.canonicalName());
        assertEquals(tbl.keyColumns().size(), tbl2.keyColumns().size());
        assertEquals(tbl.colocationColumns().size(), tbl2.colocationColumns().size());
        assertEquals(tbl.columns().size(), tbl2.columns().size());
    }

    /**
     * Validates conversion of functional default.
     */
    @Test
    public void convertFunctionalDefault() {
        final var keyColumnName = "KEY";

        TableDefinition tableDefinition = SchemaBuilders.tableBuilder("PUBLIC", "TEST")
                .columns(
                        new ColumnDefinitionImpl(
                                keyColumnName, ColumnType.string(), false,
                                DefaultValueDefinition.functionCall(DefaultValueGenerators.GEN_RANDOM_UUID)
                        ),
                        SchemaBuilders.column("VAL", ColumnType.INT8).build()
                )
                .withPrimaryKey(keyColumnName)
                .build();

        confRegistry.getConfiguration(TablesConfiguration.KEY).change(
                ch -> SchemaConfigurationConverter.createTable(tableDefinition, ch)
                        .changeTables(
                                tblsCh -> tblsCh.update(tableDefinition.canonicalName(), tblCh -> tblCh.changeReplicas(1).changeTableId(1))
                        )
        ).join();

        var tableConfiguration = confRegistry.getConfiguration(TablesConfiguration.KEY)
                .tables().get(tableDefinition.canonicalName());

        var columns = SchemaConfigurationConverter.convert(tableConfiguration.value()).columns();

        assertThat(columns, hasSize(2));
        assertThat(columns.get(0).name(), equalTo(keyColumnName));
        assertThat(columns.get(0).defaultValueDefinition(), instanceOf(FunctionCall.class));
        assertThat(((FunctionCall) columns.get(0).defaultValueDefinition()).functionName(),
                equalTo(DefaultValueGenerators.GEN_RANDOM_UUID));
    }

    /**
     * Ensures that column default are properly converted from definition to configuration and vice versa.
     *
     * @param arg Argument object describing default value to verify.
     */
    @ParameterizedTest
    @MethodSource("generateTestArguments")
    public void convertDefaults(DefaultValueArg arg) {
        final String keyColumnName = "ID";

        var columnName = arg.type.typeSpec().name();

        var tableDefinition = SchemaBuilders.tableBuilder("PUBLIC", "TEST")
                .columns(
                        SchemaBuilders.column(keyColumnName, ColumnType.INT32).build(),
                        SchemaBuilders.column(columnName, arg.type).withDefaultValue(arg.defaultValue).build()
                )
                .withPrimaryKey("ID")
                .build();

        confRegistry.getConfiguration(TablesConfiguration.KEY).change(
                ch -> SchemaConfigurationConverter.createTable(tableDefinition, ch)
                        .changeTables(
                                tblsCh -> tblsCh.update(tableDefinition.canonicalName(), tblCh -> tblCh.changeReplicas(1).changeTableId(1))
                        )
        ).join();

        var tableConfiguration = confRegistry.getConfiguration(TablesConfiguration.KEY)
                .tables().get(tableDefinition.canonicalName());

        var columns = SchemaConfigurationConverter.convert(tableConfiguration.value()).columns();

        assertThat(columns, hasSize(2));
        assertThat(columns.get(0).name(), equalTo(keyColumnName));

        var targetColumn = columns.get(1);

        assertThat(targetColumn.name(), equalTo(columnName));
        assertThat(targetColumn.type(), equalTo(arg.type));

        if (arg.defaultValue != null) {
            assertThat(targetColumn.defaultValueDefinition(), instanceOf(ConstantValue.class));
            assertThat(((ConstantValue) targetColumn.defaultValueDefinition()).value(), equalTo(arg.defaultValue));
        } else {
            assertThat(targetColumn.defaultValueDefinition(), instanceOf(DefaultValueDefinition.class));
        }
    }

    /**
     * Get tests default index configuration.
     *
     * @return Indexes configuration.
     */
    private TablesConfiguration getConfiguration() {
        return confRegistry.getConfiguration(TablesConfiguration.KEY);
    }

    private static Iterable<DefaultValueArg> generateTestArguments() {
        var paramList = new ArrayList<DefaultValueArg>();

        for (var entry : DEFAULT_VALUES_TO_TEST.entrySet()) {
            for (var defaultValue : entry.getValue()) {
                paramList.add(
                        new DefaultValueArg(specToType(entry.getKey()), adjust(defaultValue))
                );
            }
        }
        return paramList;
    }
}
