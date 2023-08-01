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

import java.util.ArrayList;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.AbstractSchemaConverterTest;
import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.DefaultValueProvider.FunctionalValueProvider;
import org.apache.ignite.internal.schema.DefaultValueProvider.Type;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.schema.VarlenNativeType;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ColumnDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ColumnDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ConstantValueDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.FunctionCallDefaultView;
import org.apache.ignite.internal.schema.configuration.defaultvalue.NullValueDefaultView;
import org.apache.ignite.internal.schema.testutils.definition.DefaultValueGenerators;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify conversion from configuration views to schema objects.
 */
@ExtendWith(ConfigurationExtension.class)
public class ConfigurationToSchemaDescriptorConverterTest extends AbstractSchemaConverterTest {
    private static final int TEST_LENGTH = 15;

    private static final int TEST_PRECISION = 8;

    private static final int TEST_SCALE = 5;

    @ParameterizedTest
    @EnumSource(value = NativeTypeSpec.class)
    public void convertColumnTypeView(NativeTypeSpec typeSpec) {
        ColumnTypeView typeView = TestColumnTypeView.forSpec(typeSpec);

        NativeType type = ConfigurationToSchemaDescriptorConverter.convert(typeView);

        assertThat(type.spec().name(), equalTo(typeView.type()));

        if (type instanceof VarlenNativeType) {
            assertThat(((VarlenNativeType) type).length(), equalTo(TEST_LENGTH));
        } else if (type instanceof NumberNativeType) {
            assertThat(((NumberNativeType) type).precision(), equalTo(typeView.precision()));
        } else if (type instanceof DecimalNativeType) {
            assertThat(((DecimalNativeType) type).precision(), equalTo(typeView.precision()));
            assertThat(((DecimalNativeType) type).scale(), equalTo(typeView.scale()));
        } else if (type instanceof TemporalNativeType) {
            assertThat(((TemporalNativeType) type).precision(), equalTo(typeView.precision()));
        } else if (type instanceof BitmaskNativeType) {
            assertThat(((BitmaskNativeType) type).bits(), equalTo(TEST_LENGTH));
        } else {
            assertThat("Unknown type: " + type.getClass(), type.getClass(), equalTo(NativeType.class));
        }
    }

    @ParameterizedTest
    @MethodSource("generateTestArguments")
    public void convertColumnViewConstantDefault(DefaultValueArg arg) {
        String columnName = arg.type.spec().name();
        ColumnTypeView typeView = TestColumnTypeView.forType(arg.type);
        ColumnDefaultView defaultValueView = createViewForValue(arg.type, arg.defaultValue);
        ColumnView columnView = new TestColumnView(columnName, typeView, defaultValueView);

        Column column = ConfigurationToSchemaDescriptorConverter.convert(0, columnView);

        assertThat(column.name(), equalTo(columnName));
        assertThat(column.type(), equalTo(arg.type));
        assertThat(column.nullable(), equalTo(arg.defaultValue == null));
        assertThat(column.defaultValueProvider().type(), equalTo(Type.CONSTANT));
        assertThat(column.defaultValue(), equalTo(arg.defaultValue));
    }

    @Test
    public void convertColumnViewFunctionalDefault() {
        String columnName = "UUID";
        String functionName = DefaultValueGenerators.GEN_RANDOM_UUID;
        ColumnTypeView typeView = TestColumnTypeView.forSpec(NativeTypeSpec.UUID);
        ColumnDefaultView defaultValueView = new TestFunctionCallDefaultView(functionName);
        ColumnView columnView = new TestColumnView(columnName, typeView, defaultValueView);

        Column column = ConfigurationToSchemaDescriptorConverter.convert(0, columnView);

        assertThat(column.name(), equalTo(columnName));
        assertThat(column.type(), equalTo(NativeTypes.UUID));
        assertThat(column.defaultValueProvider().type(), equalTo(Type.FUNCTIONAL));
        assertThat(((FunctionalValueProvider) column.defaultValueProvider()).name(), equalTo(functionName));
    }

    @Test
    public void convertTableView(@InjectConfiguration(
            value = "mock {"
                    + "primaryKey {columns=[K1, K2], colocationColumns=[K2]},"
                    + "columns: ["
                    + "     {name=C1, type.type=INT32},"
                    + "     {name=K2, type.type=INT32},"
                    + "     {name=C2, type.type=INT32},"
                    + "     {name=K1, type.type=INT32},"
                    + "]"
                    + "}"
    ) TableConfiguration tableConfiguration) {
        SchemaDescriptor schema = ConfigurationToSchemaDescriptorConverter.convert(1, tableConfiguration.value());

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

    private ColumnDefaultView createViewForValue(NativeType type, @Nullable Object value) {
        if (value == null) {
            return new TestNullValueDefaultView();
        }

        return new TestConstantValueDefaultView(ValueSerializationHelper.toString(value, type));
    }

    private static class TestColumnTypeView implements ColumnTypeView {
        private final String typeName;

        private final int length;

        private final int precision;

        private final int scale;

        public static TestColumnTypeView forSpec(NativeTypeSpec spec) {
            return new TestColumnTypeView(spec, TEST_LENGTH, TEST_PRECISION, TEST_SCALE);
        }

        public static TestColumnTypeView forType(NativeType type) {
            int length = type instanceof VarlenNativeType ? ((VarlenNativeType) type).length()
                    : type instanceof BitmaskNativeType ? ((BitmaskNativeType) type).bits()
                    : 0;

            int precision = type instanceof DecimalNativeType ? ((DecimalNativeType) type).precision()
                    : type instanceof NumberNativeType ? ((NumberNativeType) type).precision()
                    : type instanceof TemporalNativeType ? ((TemporalNativeType) type).precision()
                    : 0;

            int scale = type instanceof DecimalNativeType ? ((DecimalNativeType) type).scale() : 0;

            return new TestColumnTypeView(type.spec(), length, precision, scale);
        }

        private TestColumnTypeView(NativeTypeSpec spec, int length, int precision, int scale) {
            this.typeName = spec.name();
            this.length = length;
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public String type() {
            return typeName;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public int precision() {
            return precision;
        }

        @Override
        public int scale() {
            return scale;
        }
    }

    private static class TestColumnView implements ColumnView {
        private final String name;

        private final ColumnTypeView type;

        private final ColumnDefaultView defaultValueView;

        public TestColumnView(String name, ColumnTypeView type, ColumnDefaultView defaultValueView) {
            this.name = name;
            this.type = type;
            this.defaultValueView = defaultValueView;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ColumnTypeView type() {
            return type;
        }

        @Override
        public boolean nullable() {
            return defaultValueView instanceof NullValueDefaultView;
        }

        @Override
        public ColumnDefaultView defaultValueProvider() {
            return defaultValueView;
        }
    }

    private static class TestConstantValueDefaultView implements ConstantValueDefaultView {
        private final String value;

        public TestConstantValueDefaultView(String value) {
            this.value = value;
        }

        @Override
        public String type() {
            return ColumnDefaultConfigurationSchema.CONSTANT_VALUE_TYPE;
        }

        @Override
        public String defaultValue() {
            return value;
        }
    }

    private static class TestNullValueDefaultView implements NullValueDefaultView {
        @Override
        public String type() {
            return ColumnDefaultConfigurationSchema.CONSTANT_VALUE_TYPE;
        }
    }

    private static class TestFunctionCallDefaultView implements FunctionCallDefaultView {
        private final String functionName;

        public TestFunctionCallDefaultView(String functionName) {
            this.functionName = functionName;
        }

        @Override
        public String type() {
            return ColumnDefaultConfigurationSchema.FUNCTION_CALL_TYPE;
        }

        @Override
        public String functionName() {
            return functionName;
        }
    }
}
