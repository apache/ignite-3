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

package org.apache.ignite.internal.schema.catalog;

import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.schema.SchemaTestUtils.specToType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.schema.AbstractSchemaConverterTest;
import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.DefaultValueGenerator;
import org.apache.ignite.internal.schema.DefaultValueProvider.FunctionalValueProvider;
import org.apache.ignite.internal.schema.DefaultValueProvider.Type;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.schema.VarlenNativeType;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify conversion from catalog table descriptors to schema objects.
 */
public class CatalogToSchemaDescriptorConverterTest extends AbstractSchemaConverterTest {
    private static final int TEST_LENGTH = 15;

    private static final int TEST_PRECISION = 8;

    private static final int TEST_SCALE = 5;

    @ParameterizedTest
    @EnumSource(NativeTypeSpec.class)
    public void convertColumnType(NativeTypeSpec typeSpec) {
        CatalogTableColumnDescriptor columnDescriptor = TestColumnDescriptors.forSpec(typeSpec);

        NativeType type = CatalogToSchemaDescriptorConverter.convertType(columnDescriptor);

        if (columnDescriptor.type() == ColumnType.BYTE_ARRAY) {
            assertThat(type.spec(), is(NativeTypeSpec.BYTES));
        } else {
            assertThat(type.spec().name(), equalTo(columnDescriptor.type().name()));
        }

        if (type instanceof VarlenNativeType) {
            assertThat(((VarlenNativeType) type).length(), equalTo(TEST_LENGTH));
        } else if (type instanceof NumberNativeType) {
            assertThat(((NumberNativeType) type).precision(), equalTo(columnDescriptor.precision()));
        } else if (type instanceof DecimalNativeType) {
            assertThat(((DecimalNativeType) type).precision(), equalTo(columnDescriptor.precision()));
            assertThat(((DecimalNativeType) type).scale(), equalTo(columnDescriptor.scale()));
        } else if (type instanceof TemporalNativeType) {
            assertThat(((TemporalNativeType) type).precision(), equalTo(columnDescriptor.precision()));
        } else if (type instanceof BitmaskNativeType) {
            assertThat(((BitmaskNativeType) type).bits(), equalTo(TEST_LENGTH));
        } else {
            assertThat("Unknown type: " + type.getClass(), type.getClass(), equalTo(NativeType.class));
        }
    }

    @ParameterizedTest
    @MethodSource("generateTestArguments")
    public void convertColumnDescriptorConstantDefault(DefaultValueArg arg) {
        String columnName = arg.type.spec().name();
        CatalogTableColumnDescriptor columnDescriptor = TestColumnDescriptors.forType(arg);

        Column column = CatalogToSchemaDescriptorConverter.convert(0, columnDescriptor);

        assertThat(column.name(), equalTo(columnName));
        assertThat(column.type(), equalTo(arg.type));
        assertThat(column.nullable(), equalTo(arg.defaultValue == null));
        assertThat(column.defaultValueProvider().type(), equalTo(Type.CONSTANT));
        assertThat(column.defaultValue(), equalTo(arg.defaultValue));
    }

    @Test
    public void convertColumnDescriptorFunctionalDefault() {
        String columnName = "UUID";
        String functionName = DefaultValueGenerator.GEN_RANDOM_UUID.name();
        DefaultValue defaultValue = DefaultValue.functionCall(functionName);

        CatalogTableColumnDescriptor columnDescriptor = new CatalogTableColumnDescriptor(
                NativeTypeSpec.UUID.name(),
                NativeTypeSpec.UUID.asColumnType(),
                false,
                TEST_LENGTH,
                TEST_PRECISION,
                TEST_SCALE,
                defaultValue
        );

        Column column = CatalogToSchemaDescriptorConverter.convert(0, columnDescriptor);

        assertThat(column.name(), equalTo(columnName));
        assertThat(column.type(), equalTo(NativeTypes.UUID));
        assertThat(column.defaultValueProvider().type(), equalTo(Type.FUNCTIONAL));
        assertThat(((FunctionalValueProvider) column.defaultValueProvider()).name(), equalTo(functionName));
    }

    @Test
    public void convertTableDescriptor() {
        CatalogTableDescriptor tableDescriptor = new CatalogTableDescriptor(
                1,
                -1,
                "test",
                0,
                1,
                List.of(
                        new CatalogTableColumnDescriptor("C1", ColumnType.INT32, false, 0, 0, 0, null),
                        new CatalogTableColumnDescriptor("K2", ColumnType.INT32, false, 0, 0, 0, null),
                        new CatalogTableColumnDescriptor("C2", ColumnType.INT32, false, 0, 0, 0, null),
                        new CatalogTableColumnDescriptor("K1", ColumnType.INT32, false, 0, 0, 0, null)
                ),
                List.of("K1", "K2"),
                List.of("K2"),
                INITIAL_CAUSALITY_TOKEN
        );

        SchemaDescriptor schema = CatalogToSchemaDescriptorConverter.convert(tableDescriptor);

        assertThat(schema.keyColumns().length(), equalTo(2));
        assertThat(schema.keyColumns().column(0).name(), equalTo("K1"));
        assertThat(schema.keyColumns().column(1).name(), equalTo("K2"));
        assertThat(schema.valueColumns().length(), equalTo(2));
        assertThat(schema.valueColumns().column(0).name(), equalTo("C1"));
        assertThat(schema.valueColumns().column(1).name(), equalTo("C2"));
        assertThat(schema.colocationColumns().length, equalTo(1));
        assertThat(schema.colocationColumns()[0].name(), equalTo("K2"));
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

    private static class TestColumnDescriptors {
        static CatalogTableColumnDescriptor forSpec(NativeTypeSpec spec) {
            return new CatalogTableColumnDescriptor(spec.name(), spec.asColumnType(), false, TEST_PRECISION, TEST_SCALE, TEST_LENGTH, null);
        }

        static CatalogTableColumnDescriptor forType(DefaultValueArg arg) {
            NativeType type = arg.type;

            int length = type instanceof VarlenNativeType ? ((VarlenNativeType) type).length()
                    : type instanceof BitmaskNativeType ? ((BitmaskNativeType) type).bits()
                    : 0;

            int precision = type instanceof DecimalNativeType ? ((DecimalNativeType) type).precision()
                    : type instanceof NumberNativeType ? ((NumberNativeType) type).precision()
                    : type instanceof TemporalNativeType ? ((TemporalNativeType) type).precision()
                    : 0;

            int scale = type instanceof DecimalNativeType ? ((DecimalNativeType) type).scale() : 0;

            return new CatalogTableColumnDescriptor(
                    type.spec().name(),
                    type.spec().asColumnType(),
                    arg.defaultValue == null,
                    precision,
                    scale,
                    length,
                    DefaultValue.constant(arg.defaultValue)
            );
        }
    }
}
