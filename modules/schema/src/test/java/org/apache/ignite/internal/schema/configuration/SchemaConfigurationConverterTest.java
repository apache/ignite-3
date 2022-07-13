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

import static java.math.RoundingMode.HALF_UP;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.ColumnType.ColumnTypeSpec;
import org.apache.ignite.schema.definition.ColumnType.DecimalColumnType;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * SchemaConfigurationConverter tests.
 */
@SuppressWarnings("InstanceVariableMayNotBeInitialized")
public class SchemaConfigurationConverterTest {
    private static final Map<ColumnTypeSpec, List<Object>> DEFAULT_VALUES_TO_TEST;

    static {
        var tmp = new HashMap<ColumnTypeSpec, List<Object>>();

        tmp.put(ColumnTypeSpec.INT8, List.of(Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 14));
        tmp.put(ColumnTypeSpec.INT16, List.of(Short.MIN_VALUE, Short.MAX_VALUE, (short) 14));
        tmp.put(ColumnTypeSpec.INT32, List.of(Integer.MIN_VALUE, Integer.MAX_VALUE, 14));
        tmp.put(ColumnTypeSpec.INT64, List.of(Long.MIN_VALUE, Long.MAX_VALUE, 14L));
        tmp.put(ColumnTypeSpec.FLOAT, List.of(Float.MIN_VALUE, Float.MAX_VALUE, Float.NaN,
                Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, 14.14f));
        tmp.put(ColumnTypeSpec.DOUBLE, List.of(Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 14.14));
        tmp.put(ColumnTypeSpec.DECIMAL, List.of(BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.valueOf(Long.MIN_VALUE),
                BigDecimal.valueOf(Long.MAX_VALUE), new BigDecimal("10000000000000000000000000000000000000")));
        tmp.put(ColumnTypeSpec.DATE, List.of(LocalDate.MIN, LocalDate.MAX, LocalDate.EPOCH, LocalDate.now()));
        tmp.put(ColumnTypeSpec.TIME, List.of(LocalTime.MIN, LocalTime.MAX, LocalTime.MIDNIGHT,
                LocalTime.NOON, LocalTime.now()));
        tmp.put(ColumnTypeSpec.DATETIME, List.of(LocalDateTime.MIN, LocalDateTime.MAX, LocalDateTime.now()));
        tmp.put(ColumnTypeSpec.TIMESTAMP, List.of(Instant.MIN, Instant.MAX, Instant.EPOCH, Instant.now()));
        tmp.put(ColumnTypeSpec.UUID, List.of(UUID.randomUUID()));
        tmp.put(ColumnTypeSpec.BITMASK, List.of(fromBinString(""), fromBinString("1"), fromBinString("10101010101010101010101")));
        tmp.put(ColumnTypeSpec.STRING, List.of("", UUID.randomUUID().toString()));
        tmp.put(ColumnTypeSpec.BLOB, List.of(ArrayUtils.BYTE_EMPTY_ARRAY, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
        tmp.put(ColumnTypeSpec.NUMBER, List.of(BigInteger.ONE, BigInteger.ZERO,
                new BigInteger("10000000000000000000000000000000000000")));

        var missedTypes = new HashSet<>(Arrays.asList(ColumnTypeSpec.values()));

        missedTypes.removeAll(tmp.keySet());

        assertThat(missedTypes, empty());

        DEFAULT_VALUES_TO_TEST = Map.copyOf(tmp);
    }

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
                        SchemaBuilders.column("C", ColumnType.INT8).build()
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

        TableConfiguration tblCfg = confRegistry.getConfiguration(TablesConfiguration.KEY).tables().get(tbl.canonicalName());

        TableDefinition tbl2 = SchemaConfigurationConverter.convert(tblCfg);

        assertEquals(tbl.canonicalName(), tbl2.canonicalName());
        assertEquals(tbl.indices().size(), tbl2.indices().size());
        assertEquals(tbl.keyColumns().size(), tbl2.keyColumns().size());
        assertEquals(tbl.colocationColumns().size(), tbl2.colocationColumns().size());
        assertEquals(tbl.columns().size(), tbl2.columns().size());
    }

    /**
     * Ensures that column default are properly converted from definition to configuration and vice versa.
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
                        SchemaBuilders.column(columnName, arg.type).withDefaultValueExpression(arg.defaultValue).build()
                )
                .withPrimaryKey("ID")
                .build();

        confRegistry.getConfiguration(TablesConfiguration.KEY).change(
                ch -> SchemaConfigurationConverter.createTable(tableDefinition, ch)
                        .changeTables(
                                tblsCh -> tblsCh.createOrUpdate(tableDefinition.canonicalName(), tblCh -> tblCh.changeReplicas(1))
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
        assertThat(targetColumn.defaultValue(), equalTo(arg.defaultValue));
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

    /** Creates a column type from given type spec. */
    private static ColumnType specToType(ColumnTypeSpec spec) {
        switch (spec) {
            case INT8:
                return ColumnType.INT8;
            case INT16:
                return ColumnType.INT16;
            case INT32:
                return ColumnType.INT32;
            case INT64:
                return ColumnType.INT64;
            case FLOAT:
                return ColumnType.FLOAT;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case DECIMAL:
                return ColumnType.decimal();
            case DATE:
                return ColumnType.DATE;
            case TIME:
                return ColumnType.time();
            case DATETIME:
                return ColumnType.datetime();
            case TIMESTAMP:
                return ColumnType.timestamp();
            case NUMBER:
                return ColumnType.number();
            case STRING:
                return ColumnType.string();
            case UUID:
                return ColumnType.UUID;
            case BLOB:
                return ColumnType.blob();
            case BITMASK:
                return ColumnType.bitmaskOf(10);
            default:
                throw new IllegalStateException("Unknown type spec [spec=" + spec + ']');
        }
    }

    /** Creates a bit set from binary string. */
    private static BitSet fromBinString(String binString) {
        var bs = new BitSet();

        var idx = 0;
        for (var c : binString.toCharArray()) {
            if (c == '1') {
                bs.set(idx);
            }

            idx++;
        }

        return bs;
    }

    /**
     * Adjust the given value.
     *
     * <p>Some values need to be adjusted before comparison. For example, decimal values should be adjusted
     * in order to have the same scale, because '1.0' not equals to '1.00'.
     *
     * @param val Value to adjust.
     * @param <T> Type of te value.
     * @return Adjusted value.
     */
    @SuppressWarnings("unchecked")
    private static <T> T adjust(T val) {
        if (val instanceof BigDecimal) {
            return (T) ((BigDecimal) val).setScale(DecimalColumnType.DEFAULT_SCALE, HALF_UP);
        }

        return val;
    }

    /**
     * Converts the given value to a string representation.
     *
     * <p>Convenient method to convert a value to a string. Some types don't override
     * {@link Object#toString()} method (any array, for instance), hence should be converted to a string manually.
     */
    private static String toString(Object val) {
        if (val instanceof byte[]) {
            return Arrays.toString((byte[]) val);
        }

        return val.toString();
    }

    private static class DefaultValueArg {
        private final ColumnType type;
        private final Object defaultValue;

        public DefaultValueArg(ColumnType type, Object defaultValue) {
            this.type = type;
            this.defaultValue = defaultValue;
        }

        @Override
        public String toString() {
            return type.typeSpec() + ": " + SchemaConfigurationConverterTest.toString(defaultValue);
        }
    }
}
