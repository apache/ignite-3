/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.schema.definition.ColumnType.TemporalColumnType.DEFAULT_TIMESTAMP_PRECISION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.Period;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.PartialIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableValidator;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.ColumnType.ColumnTypeSpec;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.builder.HashIndexDefinitionBuilder;
import org.apache.ignite.schema.definition.builder.PartialIndexDefinitionBuilder;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder;
import org.apache.ignite.schema.definition.builder.TableDefinitionBuilder;
import org.apache.ignite.schema.definition.index.HashIndexDefinition;
import org.apache.ignite.schema.definition.index.IndexColumnDefinition;
import org.apache.ignite.schema.definition.index.IndexDefinition;
import org.apache.ignite.schema.definition.index.PartialIndexDefinition;
import org.apache.ignite.schema.definition.index.SortOrder;
import org.apache.ignite.schema.definition.index.SortedIndexDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * SchemaConfigurationConverter tests.
 */
@SuppressWarnings("InstanceVariableMayNotBeInitialized")
public class SchemaConfigurationConverterTest {
    /** Table builder. */
    private TableDefinitionBuilder tblBuilder;

    /** Configuration registry with one table for each test. */
    private ConfigurationRegistry confRegistry;

    /**
     * Prepare configuration registry for test.
     *
     * @throws ExecutionException If failed.
     * @throws InterruptedException If failed.
     */
    @BeforeEach
    public void createRegistry() throws ExecutionException, InterruptedException {
        confRegistry = new ConfigurationRegistry(
                List.of(TablesConfiguration.KEY),
                Map.of(TableValidator.class, Set.of(TableValidatorImpl.INSTANCE)),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of(
                        HashIndexConfigurationSchema.class,
                        SortedIndexConfigurationSchema.class,
                        PartialIndexConfigurationSchema.class,
                        UnknownDataStorageConfigurationSchema.class
                )
        );

        confRegistry.start();

        tblBuilder = SchemaBuilders.tableBuilder("SNAME", "TNAME")
                .columns(
                        SchemaBuilders.column("COL1", ColumnType.DOUBLE).build(),
                        SchemaBuilders.column("COL2", ColumnType.DOUBLE).build(),
                        SchemaBuilders.column("A", ColumnType.INT8).build(),
                        SchemaBuilders.column("B", ColumnType.INT8).build(),
                        SchemaBuilders.column("C", ColumnType.INT8).build(),
                        SchemaBuilders.column("DUR", ColumnType.duration()).build(),
                        SchemaBuilders.column("PER", ColumnType.PERIOD).build()
                ).withPrimaryKey("COL1");

        TableDefinition tbl = tblBuilder.build();

        confRegistry.getConfiguration(TablesConfiguration.KEY).change(
                ch -> SchemaConfigurationConverter.createTable(tbl, ch)
                        .changeTables(
                                tblsCh -> tblsCh.createOrUpdate(tbl.canonicalName(), tblCh -> tblCh.changeReplicas(1))
                        )
        ).get();
    }

    @AfterEach
    void tearDown() throws Exception {
        confRegistry.stop();
    }

    /**
     * Test conversion correctness of all available ColumnType`s.
     * If additional type will be added - assertion will be raised and test improvement will be required.
     */
    @Test
    public void testConverterCorrectness() throws Exception {
        var uuid = UUID.randomUUID();
        var date = LocalDate.of(1995, Month.MAY, 23);
        var time = LocalTime.of(17, 0, 1, 222_333_444);
        var datetime = LocalDateTime.of(1995, Month.MAY, 23, 17, 0, 1, 222_333_444);
        var timestamp = Instant.now();
        var duration = Duration.ofMinutes(1);
        var period = Period.ofDays(2);
        var bytes = new byte[]{1, 2};
        var bi = BigInteger.valueOf(10);
        var bd = BigDecimal.valueOf(22);

        Map<String, Object> data = new HashMap<>();
        data.put("int8", (byte) 1);
        data.put("int16", (short) 2);
        data.put("int32", 3);
        data.put("int64", (long) 4);
        data.put("float", (float) 5.5);
        data.put("double", 6.6);
        data.put("decimal", bd);
        data.put("uuid", uuid);
        data.put("string", "8");
        data.put("bytes", bytes);
        data.put("bitmask", new BitSet(3));
        data.put("date", date);
        data.put("time", time);
        data.put("datetime", datetime);
        data.put("timestamp", timestamp);
        data.put("number", bi);
        data.put("duration", duration);
        data.put("period", period);

        List<ColumnDefinition> allColumns = new ArrayList<>();

        allColumns.add(SchemaBuilders.column("ID", ColumnType.INT32).build());
        allColumns.add(SchemaBuilders.column("int8", ColumnType.INT8).asNullable(true)
                .withDefaultValueExpression(data.get("int8")).build());
        allColumns.add(SchemaBuilders.column("int16", ColumnType.INT16).asNullable(true)
                .withDefaultValueExpression(data.get("int16")).build());
        allColumns.add(SchemaBuilders.column("int32", ColumnType.INT32).asNullable(true)
                .withDefaultValueExpression(data.get("int32")).build());
        allColumns.add(SchemaBuilders.column("int64", ColumnType.INT64).asNullable(true)
                .withDefaultValueExpression(data.get("int64")).build());
        allColumns.add(SchemaBuilders.column("float", ColumnType.FLOAT).asNullable(true)
                .withDefaultValueExpression(data.get("float")).build());
        allColumns.add(SchemaBuilders.column("double", ColumnType.DOUBLE).asNullable(true)
                .withDefaultValueExpression(data.get("double")).build());
        allColumns.add(SchemaBuilders.column("number", ColumnType.numberOf(2)).asNullable(true)
                .withDefaultValueExpression(data.get("number")).build());
        //not supported yet
        //SchemaBuilders.column("UUIDVAL" ...
        //SchemaBuilders.column("BITSET" ...
        allColumns.add(SchemaBuilders.column("date", ColumnType.DATE).asNullable(true)
                .withDefaultValueExpression(data.get("date")).build());
        allColumns.add(SchemaBuilders.column("period", ColumnType.PERIOD).asNullable(true)
                .withDefaultValueExpression(data.get("period")).build());
        allColumns.add(SchemaBuilders.column("decimal", ColumnType.decimalOf()).asNullable(true)
                .withDefaultValueExpression(data.get("decimal")).build());
        allColumns.add(SchemaBuilders.column("string", ColumnType.string()).asNullable(true)
                .withDefaultValueExpression(data.get("string")).build());
        allColumns.add(SchemaBuilders.column("bytes", ColumnType.blobOf()).asNullable(true).build());
        allColumns.add(SchemaBuilders.column("time", ColumnType.TemporalColumnType.time()).asNullable(true)
                .withDefaultValueExpression(data.get("time")).build());
        allColumns.add(SchemaBuilders.column("datetime",
                ColumnType.TemporalColumnType.datetime()).asNullable(true).withDefaultValueExpression(data.get("datetime")).build());
        allColumns.add(SchemaBuilders.column("timestamp",
                ColumnType.TemporalColumnType.timestamp()).asNullable(true).withDefaultValueExpression(data.get("timestamp")).build());
        allColumns.add(SchemaBuilders.column("duration", ColumnType.duration(DEFAULT_TIMESTAMP_PRECISION)).asNullable(true)
                .withDefaultValueExpression(data.get("duration")).build());

        // exclude types : ColumnType.UINTx and not supported, -ColumnType.BOOLEAN
        assertEquals(allColumns.size() + 4 + 1, ColumnTypeSpec.values().length, "Some data types are not covered.");

        TableDefinitionBuilder allTypesTblBuilder = SchemaBuilders.tableBuilder("SNAME", "ALLTYPES")
                .columns(allColumns).withPrimaryKey("ID");

        TableDefinition tbl = allTypesTblBuilder.build();

        confRegistry.getConfiguration(TablesConfiguration.KEY).change(
                ch -> SchemaConfigurationConverter.createTable(tbl, ch)
                        .changeTables(
                                tblsCh -> tblsCh.createOrUpdate(tbl.canonicalName(), tblCh -> tblCh.changeReplicas(1))
                        )
        ).get();

        TableConfiguration tblConf = confRegistry.getConfiguration(TablesConfiguration.KEY).tables()
                .get(allTypesTblBuilder.build().canonicalName());

        TableDefinition tblDef = SchemaConfigurationConverter.convert(tblConf.value());

        SchemaDescriptor schema = SchemaDescriptorConverter.convert(100, tblDef);

        Columns cols = schema.valueColumns();

        for (Column col : cols.columns()) {
            String colName = col.name().toLowerCase();
            Object expVal = data.get(colName);
            assertNotNull(expVal, "Default missed for: " + colName);

            col.validate(expVal);
            assertTrue(col.nullable());

            // blob has no default for now.
            if ("bytes".equals(colName)) {
                continue;
            }

            if ("decimal".equals(colName)) {
                ((BigDecimal) expVal).compareTo((BigDecimal) col.defaultValue());

                continue;
            }

            assertEquals(expVal, col.defaultValue(), "Column name=" + colName + " has no expected default.");
        }
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

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();

        TableDefinition tbl = SchemaConfigurationConverter.convert(getTbl().value());

        HashIndexDefinition idx2 = (HashIndexDefinition) getIdx(idx.name(), tbl.indices());

        assertNotNull(idx2);
        assertEquals("HASH", idx2.type());
        assertEquals(3, idx2.columns().size());
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

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();

        TableDefinition tbl = SchemaConfigurationConverter.convert(getTbl().value());

        SortedIndexDefinition idx2 = (SortedIndexDefinition) getIdx(idx.name(), tbl.indices());

        assertNotNull(idx2);
        assertEquals("SORTED", idx2.type());
        assertEquals(2, idx2.columns().size());
        assertEquals("A", idx2.columns().get(0).name());
        assertEquals("B", idx2.columns().get(1).name());
        assertEquals(SortOrder.ASC, idx2.columns().get(0).sortOrder());
        assertEquals(SortOrder.DESC, idx2.columns().get(1).sortOrder());
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

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();

        TableDefinition tbl = SchemaConfigurationConverter.convert(getTbl().value());

        SortedIndexDefinition idx2 = (SortedIndexDefinition) getIdx(idx.name(), tbl.indices());

        assertNotNull(idx2);
        assertEquals("PK_SORTED", idx2.name());
        assertEquals("SORTED", idx2.type());
        assertEquals(idx.columns().stream().map(IndexColumnDefinition::name).collect(Collectors.toList()),
                idx2.columns().stream().map(IndexColumnDefinition::name).collect(Collectors.toList()));
        assertTrue(idx2.unique());
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

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();

        TableDefinition tbl = SchemaConfigurationConverter.convert(getTbl().value());

        SortedIndexDefinition idx2 = (SortedIndexDefinition) getIdx(idx.name(), tbl.indices());

        assertNotNull(idx2);
        assertEquals("uniq_sorted", idx2.name());
        assertEquals("SORTED", idx2.type());

        assertTrue(idx2.unique());

        assertEquals(2, idx2.columns().size());
        assertEquals("A", idx2.columns().get(0).name());
        assertEquals("COL1", idx2.columns().get(1).name());
        assertEquals(SortOrder.ASC, idx2.columns().get(0).sortOrder());
        assertEquals(SortOrder.DESC, idx2.columns().get(1).sortOrder());
    }

    /**
     * Add/remove PartialIndex into configuration and read it back.
     */
    @Test
    public void testPartialIndex() throws Exception {
        PartialIndexDefinitionBuilder builder = SchemaBuilders.partialIndex("TEST");

        builder.addIndexColumn("A").done();
        builder.withExpression("WHERE A > 0");

        PartialIndexDefinition idx = builder.build();

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();

        TableDefinition tbl = SchemaConfigurationConverter.convert(getTbl().value());

        PartialIndexDefinition idx2 = (PartialIndexDefinition) getIdx(idx.name(), tbl.indices());

        assertNotNull(idx2);
        assertEquals("PARTIAL", idx2.type());
        assertEquals(idx.columns().size(), idx2.columns().size());
    }

    /**
     * Add/remove table and read it back.
     */
    @Test
    public void testConvertTable() {
        TableDefinition tbl = tblBuilder.build();

        TableConfiguration tblCfg = getTbl();

        TableDefinition tbl2 = SchemaConfigurationConverter.convert(tblCfg);

        assertEquals(tbl.canonicalName(), tbl2.canonicalName());
        assertEquals(tbl.indices().size(), tbl2.indices().size());
        assertEquals(tbl.keyColumns().size(), tbl2.keyColumns().size());
        assertEquals(tbl.colocationColumns().size(), tbl2.colocationColumns().size());
        assertEquals(tbl.columns().size(), tbl2.columns().size());
    }

    /**
     * Get tests default table configuration.
     *
     * @return Configuration of default table.
     */
    private TableConfiguration getTbl() {
        return confRegistry.getConfiguration(TablesConfiguration.KEY).tables().get(tblBuilder.build().canonicalName());
    }

    /**
     * Get table index by name.
     *
     * @param name Index name to find.
     * @param idxs Table indexes.
     * @return Index or {@code null} if there are no index with such name.
     */
    private IndexDefinition getIdx(String name, Collection<IndexDefinition> idxs) {
        return idxs.stream().filter(idx -> name.equals(idx.name())).findAny().orElse(null);
    }
}
