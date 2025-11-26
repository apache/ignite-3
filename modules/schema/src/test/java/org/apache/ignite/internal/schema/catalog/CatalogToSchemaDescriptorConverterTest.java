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

import static org.apache.ignite.internal.schema.SchemaTestUtils.specToType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.schema.AbstractSchemaConverterTest;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DefaultValueGenerator;
import org.apache.ignite.internal.schema.DefaultValueProvider.FunctionalValueProvider;
import org.apache.ignite.internal.schema.DefaultValueProvider.Type;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify conversion from catalog table descriptors to schema objects.
 */
public class CatalogToSchemaDescriptorConverterTest extends AbstractSchemaConverterTest {
    private static final int TEST_LENGTH = 15;

    private static final int TEST_PRECISION = 8;

    private static final int TEST_SCALE = 5;

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "PERIOD", "DURATION", "STRUCT"}, mode = Mode.EXCLUDE)
    public void convertColumnType(ColumnType typeSpec) {
        CatalogTableColumnDescriptor columnDescriptor = TestColumnDescriptors.forSpec(typeSpec);

        NativeType type = CatalogToSchemaDescriptorConverter.convertType(columnDescriptor);

        if (columnDescriptor.type() == ColumnType.BYTE_ARRAY) {
            assertThat(type.spec(), is(ColumnType.BYTE_ARRAY));
        } else {
            assertThat(type.spec().name(), equalTo(columnDescriptor.type().name()));
        }

        if (type instanceof VarlenNativeType) {
            assertThat(((VarlenNativeType) type).length(), equalTo(TEST_LENGTH));
        } else if (type instanceof DecimalNativeType) {
            assertThat(((DecimalNativeType) type).precision(), equalTo(columnDescriptor.precision()));
            assertThat(((DecimalNativeType) type).scale(), equalTo(columnDescriptor.scale()));
        } else if (type instanceof TemporalNativeType) {
            assertThat(((TemporalNativeType) type).precision(), equalTo(columnDescriptor.precision()));
        } else {
            assertThat("Unknown type: " + type.getClass(), type.getClass(), equalTo(NativeType.class));
        }
    }

    @ParameterizedTest
    @MethodSource("generateTestArguments")
    public void convertColumnDescriptorConstantDefault(DefaultValueArg arg) {
        String columnName = arg.type.spec().name();
        CatalogTableColumnDescriptor columnDescriptor = TestColumnDescriptors.forType(arg);

        Column column = CatalogToSchemaDescriptorConverter.convert(columnDescriptor);

        assertThat(column.name(), equalTo(columnName));
        assertThat(column.type(), equalTo(arg.type));
        assertThat(column.nullable(), equalTo(arg.defaultValue == null));
        assertThat(column.defaultValueProvider().type(), equalTo(Type.CONSTANT));
        assertThat(column.defaultValue(), equalTo(arg.defaultValue));
    }

    @Test
    public void convertColumnDescriptorFunctionalDefault() {
        String columnName = "UUID";
        String functionName = DefaultValueGenerator.RAND_UUID.name();
        DefaultValue defaultValue = DefaultValue.functionCall(functionName);

        CatalogTableColumnDescriptor columnDescriptor = new CatalogTableColumnDescriptor(
                ColumnType.UUID.name(),
                ColumnType.UUID,
                false,
                TEST_LENGTH,
                TEST_PRECISION,
                TEST_SCALE,
                defaultValue
        );

        Column column = CatalogToSchemaDescriptorConverter.convert(columnDescriptor);

        assertThat(column.name(), equalTo(columnName));
        assertThat(column.type(), equalTo(NativeTypes.UUID));
        assertThat(column.defaultValueProvider().type(), equalTo(Type.FUNCTIONAL));
        assertThat(((FunctionalValueProvider) column.defaultValueProvider()).name(), equalTo(functionName));
    }

    @Test
    public void convertTableDescriptor() {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("C1", ColumnType.INT32, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("K2", ColumnType.INT32, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("C2", ColumnType.INT32, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("K1", ColumnType.INT32, false, 0, 0, 0, null)
        );
        CatalogTableDescriptor tableDescriptor = CatalogTableDescriptor.builder()
                .id(1)
                .schemaId(-1)
                .primaryKeyIndexId(-1)
                .name("test")
                .zoneId(0)
                .newColumns(columns)
                .primaryKeyColumns(IntList.of(3, 1))
                .colocationColumns(IntList.of(1))
                .storageProfile(CatalogService.DEFAULT_STORAGE_PROFILE)
                .build();

        SchemaDescriptor schema = CatalogToSchemaDescriptorConverter.convert(tableDescriptor, tableDescriptor.latestSchemaVersion());

        assertThat(schema.keyColumns().size(), equalTo(2));
        assertThat(schema.keyColumns().get(0).name(), equalTo("K1"));
        assertThat(schema.keyColumns().get(1).name(), equalTo("K2"));
        assertThat(schema.valueColumns().size(), equalTo(2));
        assertThat(schema.valueColumns().get(0).name(), equalTo("C1"));
        assertThat(schema.valueColumns().get(1).name(), equalTo("C2"));
        assertThat(schema.colocationColumns().size(), equalTo(1));
        assertThat(schema.colocationColumns().get(0).name(), equalTo("K2"));
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
        static CatalogTableColumnDescriptor forSpec(ColumnType spec) {
            return new CatalogTableColumnDescriptor(spec.name(), spec, false, TEST_PRECISION, TEST_SCALE, TEST_LENGTH, null);
        }

        static CatalogTableColumnDescriptor forType(DefaultValueArg arg) {
            NativeType type = arg.type;

            int length = type instanceof VarlenNativeType ? ((VarlenNativeType) type).length() : 0;

            int precision = type instanceof DecimalNativeType ? ((DecimalNativeType) type).precision()
                    : type instanceof TemporalNativeType ? ((TemporalNativeType) type).precision()
                    : 0;

            int scale = type instanceof DecimalNativeType ? ((DecimalNativeType) type).scale() : 0;

            return new CatalogTableColumnDescriptor(
                    type.spec().name(),
                    type.spec(),
                    arg.defaultValue == null,
                    precision,
                    scale,
                    length,
                    DefaultValue.constant(arg.defaultValue)
            );
        }
    }
}
