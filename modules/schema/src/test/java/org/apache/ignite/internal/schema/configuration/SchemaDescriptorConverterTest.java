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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.DefaultValueGenerator;
import org.apache.ignite.internal.schema.DefaultValueProvider.FunctionalValueProvider;
import org.apache.ignite.internal.schema.DefaultValueProvider.Type;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.definition.ColumnDefinitionImpl;
import org.apache.ignite.internal.schema.testutils.builder.ColumnDefinitionBuilder;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.builder.TableDefinitionBuilder;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.DefaultValueDefinition;
import org.apache.ignite.schema.definition.DefaultValueGenerators;
import org.apache.ignite.schema.definition.TableDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for SchemaDescriptorConverter.
 */
public class SchemaDescriptorConverterTest extends AbstractSchemaConverterTest {
    /** Total number of columns. */
    private static final int columns = 15;

    /**
     * Convert table with complex primary key and check it.
     */
    @Test
    public void testComplexPrimaryKey() {
        TableDefinitionBuilder bldr = getBuilder(false, false);
        TableDefinition tblSchm = bldr.withPrimaryKey(
                SchemaBuilders.primaryKey()
                        .withColumns("INT8", "ID")
                        .build()
        ).build();

        SchemaDescriptor tblDscr = SchemaDescriptorConverter.convert(1, tblSchm);

        assertEquals(2, tblDscr.keyColumns().length());
        assertEquals(2, tblDscr.colocationColumns().length);
        assertEquals(columns - 2, tblDscr.valueColumns().length());
    }

    /**
     * Convert table with complex primary key with affinity column configured and check it.
     */
    @Test
    public void testComplexPrimaryKeyWithAffinity() {
        TableDefinitionBuilder bldr = getBuilder(false, false);
        TableDefinition tblSchm = bldr.withPrimaryKey(
                SchemaBuilders.primaryKey()
                        .withColumns("INT8", "ID")
                        .withColocationColumns("INT8")
                        .build()
        ).build();

        SchemaDescriptor tblDscr = SchemaDescriptorConverter.convert(1, tblSchm);

        assertEquals(2, tblDscr.keyColumns().length());
        assertEquals(1, tblDscr.colocationColumns().length);
        assertEquals(columns - 2, tblDscr.valueColumns().length());
    }

    /**
     * Convert table with nullable columns.
     */
    @Test
    public void convertNullable() {
        TableDefinition tblSchm = getBuilder(true, true).build();

        SchemaDescriptor tblDscr = SchemaDescriptorConverter.convert(1, tblSchm);

        assertEquals(1, tblDscr.keyColumns().length());
        testCol(tblDscr.keyColumns(), "ID", NativeTypeSpec.UUID, false);

        assertEquals(columns - 1, tblDscr.valueColumns().length());
        testCol(tblDscr.valueColumns(), "INT8", NativeTypeSpec.INT8, true);
        testCol(tblDscr.valueColumns(), "INT16", NativeTypeSpec.INT16, true);
        testCol(tblDscr.valueColumns(), "INT32", NativeTypeSpec.INT32, true);
        testCol(tblDscr.valueColumns(), "INT64", NativeTypeSpec.INT64, true);
        testCol(tblDscr.valueColumns(), "FLOAT", NativeTypeSpec.FLOAT, true);
        testCol(tblDscr.valueColumns(), "DOUBLE", NativeTypeSpec.DOUBLE, true);
        testCol(tblDscr.valueColumns(), "UUID", NativeTypeSpec.UUID, true);
        testCol(tblDscr.valueColumns(), "STRING", NativeTypeSpec.STRING, true);
        testCol(tblDscr.valueColumns(), "STRING_FS10", NativeTypeSpec.STRING, true);
        testCol(tblDscr.valueColumns(), "BLOB", NativeTypeSpec.BYTES, true);
        testCol(tblDscr.valueColumns(), "DECIMAL", NativeTypeSpec.DECIMAL, true);
        testCol(tblDscr.valueColumns(), "NUMBER", NativeTypeSpec.NUMBER, true);
        testCol(tblDscr.valueColumns(), "DECIMAL", NativeTypeSpec.DECIMAL, true);
        testCol(tblDscr.valueColumns(), "BITMASK_FS10", NativeTypeSpec.BITMASK, true);
    }

    @ParameterizedTest
    @MethodSource("generateTestArguments")
    public void convertDefaultValues(DefaultValueArg arg) {
        final String keyColumnName = "ID";

        var columnName = arg.type.typeSpec().name();
        var nullable = arg.defaultValue == null;

        var tableDefinition = SchemaBuilders.tableBuilder("PUBLIC", "TEST")
                .columns(
                        SchemaBuilders.column(keyColumnName, ColumnType.INT32).build(),
                        SchemaBuilders.column(columnName, arg.type)
                                .withDefaultValue(arg.defaultValue)
                                .asNullable(nullable)
                                .build()
                )
                .withPrimaryKey(keyColumnName)
                .build();

        SchemaDescriptor schemaDescriptor = SchemaDescriptorConverter.convert(1, tableDefinition);

        assertEquals(1, schemaDescriptor.valueColumns().length());

        var column = schemaDescriptor.valueColumns().columns()[0];

        assertThat(column.name(), equalTo(columnName));
        assertThat(column.nullable(), equalTo(nullable));
        assertThat(column.defaultValueProvider().type(), equalTo(Type.CONSTANT));
        assertThat(column.defaultValue(), equalTo(arg.defaultValue));
    }

    /**
     * Validates conversion of functional default.
     */
    @Test
    public void convertFunctionalDefault() {
        final String keyColumnName = "ID";

        var tableDefinition = SchemaBuilders.tableBuilder("PUBLIC", "TEST")
                .columns(
                        new ColumnDefinitionImpl(
                                keyColumnName, ColumnType.string(), false,
                                DefaultValueDefinition.functionCall(DefaultValueGenerators.GEN_RANDOM_UUID)
                        ),
                        SchemaBuilders.column("VAL", ColumnType.INT8).asNullable(true).build()
                )
                .withPrimaryKey(keyColumnName)
                .build();

        SchemaDescriptor schemaDescriptor = SchemaDescriptorConverter.convert(1, tableDefinition);

        assertEquals(1, schemaDescriptor.keyColumns().length());

        var column = schemaDescriptor.keyColumns().columns()[0];

        assertThat(column.name(), equalTo(keyColumnName));
        assertThat(column.nullable(), equalTo(false));
        assertThat(column.defaultValueProvider().type(), equalTo(Type.FUNCTIONAL));
        assertThat(((FunctionalValueProvider) column.defaultValueProvider()).name(), equalTo(DefaultValueGenerator.GEN_RANDOM_UUID.name()));
    }

    /**
     * Convert table with complex primary key and check it.
     */
    @Test
    public void testColumnOrder() {
        ColumnDefinition[] cols = {
                SchemaBuilders.column("ID", ColumnType.UUID).build(),
                SchemaBuilders.column("STRING", ColumnType.string()).build(),
                SchemaBuilders.column("INT32", ColumnType.INT32).build(),
                SchemaBuilders.column("INT64", ColumnType.INT64).build(),
                SchemaBuilders.column("DOUBLE", ColumnType.DOUBLE).build(),
                SchemaBuilders.column("UUID", ColumnType.UUID).build(),
                SchemaBuilders.column("INT16", ColumnType.INT16).build(),
                SchemaBuilders.column("BITMASK_FS10", ColumnType.bitmaskOf(10)).build()
        };

        TableDefinition tblSchm = SchemaBuilders.tableBuilder("SCHEMA", "TABLE")
                .columns(cols)
                .withPrimaryKey(
                        SchemaBuilders.primaryKey()
                                .withColumns("INT32", "ID")
                                .withColocationColumns("INT32")
                                .build()
                ).build();

        SchemaDescriptor tblDscr = SchemaDescriptorConverter.convert(1, tblSchm);

        for (int i = 0; i < cols.length; i++) {
            Column col = tblDscr.column(i);

            assertEquals(col.name(), cols[col.columnOrder()].name());
        }

        assertArrayEquals(Arrays.stream(cols).map(ColumnDefinition::name).toArray(String[]::new),
                tblDscr.columnNames().toArray(String[]::new));
    }

    /**
     * Get TableSchemaBuilder with default table.
     *
     * @param nullable If all columns should be nullable.
     * @param withPk   If builder should contains primary key index.
     * @return TableSchemaBuilder.
     */
    private TableDefinitionBuilder getBuilder(boolean nullable, boolean withPk) {
        Function<ColumnDefinitionBuilder, ColumnDefinition> postProcess =
                builder -> builder.asNullable(nullable).build();

        TableDefinitionBuilder res = SchemaBuilders.tableBuilder("SCHEMA", "TABLE")
                .columns(
                        SchemaBuilders.column("ID", ColumnType.UUID).build(),
                        postProcess.apply(SchemaBuilders.column("INT8", ColumnType.INT8)),
                        postProcess.apply(SchemaBuilders.column("INT16", ColumnType.INT16)),
                        postProcess.apply(SchemaBuilders.column("INT32", ColumnType.INT32)),
                        postProcess.apply(SchemaBuilders.column("INT64", ColumnType.INT64)),
                        postProcess.apply(SchemaBuilders.column("FLOAT", ColumnType.FLOAT)),
                        postProcess.apply(SchemaBuilders.column("DOUBLE", ColumnType.DOUBLE)),
                        postProcess.apply(SchemaBuilders.column("UUID", ColumnType.UUID)),
                        postProcess.apply(SchemaBuilders.column("STRING", ColumnType.string())),
                        postProcess.apply(SchemaBuilders.column("STRING_FS10", ColumnType.stringOf(10))),
                        postProcess.apply(SchemaBuilders.column("BLOB", ColumnType.blob())),
                        postProcess.apply(SchemaBuilders.column("BLOB_FS10", ColumnType.blobOf(10))),
                        postProcess.apply(SchemaBuilders.column("DECIMAL", ColumnType.decimalOf(1, 1))),
                        postProcess.apply(SchemaBuilders.column("NUMBER", ColumnType.numberOf(12))),
                        postProcess.apply(SchemaBuilders.column("BITMASK_FS10", ColumnType.bitmaskOf(10)))
                // TODO: IGNITE-13750 uncomment after unsigned types available
                // postProcess.apply(SchemaBuilders.column("UINT8", ColumnType.UINT8)),
                // postProcess.apply(SchemaBuilders.column("UINT16", ColumnType.UINT16)),
                // postProcess.apply(SchemaBuilders.column("UINT32", ColumnType.UINT32)),
                // postProcess.apply(SchemaBuilders.column("UINT64", ColumnType.UINT64)),
                );
        if (withPk) {
            res.withPrimaryKey("ID");
        }

        return res;
    }

    /**
     * Check specified column to match other parameters.
     *
     * @param cols     Columns to test.
     * @param name     Expected column name.
     * @param type     Expected column type.
     * @param nullable Expected column nullable flag.
     */
    private static void testCol(Columns cols, String name, NativeTypeSpec type, boolean nullable) {
        int idx = cols.columnIndex(name);
        Column col = cols.column(idx);

        assertEquals(name, col.name());
        assertEquals(type.name(), col.type().spec().name());
        assertEquals(nullable, col.nullable());

        if (col.type().spec().fixedLength()) {
            assertTrue(col.type().sizeInBytes() >= 0);
        }
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
