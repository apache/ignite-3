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

package org.apache.ignite.internal.marshaller;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests for {@link ReflectionMarshallersProvider}. */
public class ReflectionMarshallersProviderSelfTest {

    private final MarshallersProvider marshallers = new ReflectionMarshallersProvider();

    @ParameterizedTest
    @EnumSource(MarshallerType.class)
    public void testMarshallerCache(MarshallerType marshallerType) {
        Mapper<TestPoJo> mapper = Mapper.of(TestPoJo.class);
        List<String> fieldOrder = List.of("col1", "col2", "col3");

        // This test assumes that Mappers are cached.

        TestMarshallerSchema schema1 = new MarshallerSchemaBuilder(fieldOrder)
                .version(1)
                .addKey("col1", BinaryMode.INT)
                .addValue("col2", BinaryMode.INT)
                .build();

        // Same schema - same versions, same content

        {
            Marshaller m1 = marshallerType.get(marshallers, schema1, mapper, false, true);
            Marshaller m2 = marshallerType.get(marshallers, schema1, mapper, false, true);
            Marshaller m3 = marshallerType.get(marshallers, schema1, mapper, true, true);

            assertSame(m1, m2);
            assertNotSame(m1, m3);
            assertNotSame(m2, m3);
        }

        TestMarshallerSchema schema2 = new MarshallerSchemaBuilder(fieldOrder)
                .version(schema1.version + 1)
                .addKey("col1", BinaryMode.INT)
                .addValue("col2", BinaryMode.INT)
                .addValue("col3", BinaryMode.INT)
                .build();

        // Different schemas - different versions, different content
        {
            Marshaller m1 = marshallerType.get(marshallers, schema1, mapper, false, true);
            Marshaller m2 = marshallerType.get(marshallers, schema2, mapper, false, true);
            Marshaller m3 = marshallerType.get(marshallers, schema2, mapper, true, true);

            assertNotSame(m1, m2);
            assertNotSame(m1, m3);
        }

        TestMarshallerSchema schema3 = new MarshallerSchemaBuilder(fieldOrder)
                .version(schema2.version + 1)
                .addKey("col1", BinaryMode.INT)
                .addValue("col2", BinaryMode.INT)
                .build();

        // Different schemas - different versions, same content
        {
            Marshaller m1 = marshallerType.get(marshallers, schema1, mapper, false, true);
            Marshaller m2 = marshallerType.get(marshallers, schema3, mapper, false, true);
            Marshaller m3 = marshallerType.get(marshallers, schema3, mapper, true, true);

            if (marshallerType.cacheBySchemaColumns()) {
                assertSame(m1, m2);
                assertNotSame(m1, m3);
            } else {
                assertNotSame(m1, m2);
                assertNotSame(m1, m3);
            }
        }
    }

    @Test
    public void testKeyMarshallerCacheDiffOrder() {
        Mapper<TestPoJo> mapper = Mapper.of(TestPoJo.class);
        List<String> fieldOrder = List.of("col1", "col2", "col3", "col4");

        TestMarshallerSchema schema1 = new MarshallerSchemaBuilder(fieldOrder)
                .version(1)
                .addKey("col1", BinaryMode.INT)
                .addKey("col2", BinaryMode.INT)
                .addValue("col3", BinaryMode.INT)
                .addValue("col4", BinaryMode.INT)
                .build();

        TestMarshallerSchema schema2 = new MarshallerSchemaBuilder(List.of("col3", "col2", "col1", "col4"))
                .version(1)
                .addKey("col1", BinaryMode.INT)
                .addKey("col2", BinaryMode.INT)
                .addValue("col3", BinaryMode.INT)
                .addValue("col4", BinaryMode.INT)
                .build();

        Marshaller m1 = MarshallerType.KEYS.get(marshallers, schema1, mapper, false, true);
        Marshaller m2 = MarshallerType.KEYS.get(marshallers, schema2, mapper, false, true);

        assertNotSame(m1, m2);
    }

    @Test
    public void testValueMarshallerCacheDiffOrder() {
        Mapper<TestPoJo> mapper = Mapper.of(TestPoJo.class);
        List<String> fieldOrder = List.of("col1", "col2", "col3", "col4");

        TestMarshallerSchema schema1 = new MarshallerSchemaBuilder(fieldOrder)
                .version(1)
                .addKey("col1", BinaryMode.INT)
                .addKey("col2", BinaryMode.INT)
                .addValue("col3", BinaryMode.INT)
                .addValue("col4", BinaryMode.INT)
                .build();

        TestMarshallerSchema schema2 = new MarshallerSchemaBuilder(List.of("col1", "col4", "col3", "col3"))
                .version(1)
                .addKey("col1", BinaryMode.INT)
                .addKey("col2", BinaryMode.INT)
                .addValue("col3", BinaryMode.INT)
                .addValue("col4", BinaryMode.INT)
                .build();

        Marshaller m1 = MarshallerType.KEYS.get(marshallers, schema1, mapper, false, true);
        Marshaller m2 = MarshallerType.KEYS.get(marshallers, schema2, mapper, false, true);

        assertNotSame(m1, m2);
    }

    enum MarshallerType {
        /** Uses only key columns. */
        KEYS,
        /** Uses only values columns. */
        VALUES,
        /** All schema columns. */
        ROW,
        /** Arbitrary columns. */
        PROJECTION
        ;

        Marshaller get(MarshallersProvider marshallers, TestMarshallerSchema schema,
                Mapper<?> mapper,
                boolean requireAllFields,
                boolean allowUnmappedFields) {

            switch (this) {
                case KEYS:
                    return marshallers.getKeysMarshaller(schema.schema, mapper, requireAllFields, allowUnmappedFields);
                case VALUES:
                    return marshallers.getValuesMarshaller(schema.schema, mapper, requireAllFields, allowUnmappedFields);
                case ROW:
                    return marshallers.getRowMarshaller(schema.schema, mapper, requireAllFields, allowUnmappedFields);
                case PROJECTION:
                    return marshallers.getMarshaller(schema.columns, mapper, requireAllFields, allowUnmappedFields);
                default:
                    throw new UnsupportedOperationException("Unexpected marshaller type " + this);
            }
        }

        boolean cacheBySchemaColumns() {
            return PROJECTION == this;
        }
    }

    @SuppressWarnings("unused")
    private static class TestPoJo {

        private int col1;

        private int col2;

        private int col3;
    }

    private static class TestMarshallerSchema {

        private final MarshallerSchema schema;

        private final MarshallerColumn[] columns;

        private final int version;

        TestMarshallerSchema(MarshallerSchema schema, MarshallerColumn[] columns) {
            this.schema = schema;
            this.columns = columns;
            this.version = schema.schemaVersion();
        }
    }

    private static class MarshallerSchemaBuilder {

        private int version;

        private final List<MarshallerColumn> keys = new ArrayList<>();

        private final List<MarshallerColumn> values = new ArrayList<>();

        private final List<String> fieldOrder;

        private MarshallerSchemaBuilder(List<String> fieldOrder) {
            this.fieldOrder = fieldOrder;
        }

        MarshallerSchemaBuilder version(int version) {
            this.version = version;
            return this;
        }

        MarshallerSchemaBuilder addKey(String name, BinaryMode binaryMode) {
            int schemaIndex = fieldOrder.indexOf(name);
            keys.add(new MarshallerColumn(schemaIndex, name.toUpperCase(Locale.US), binaryMode, null, 0));
            return this;
        }

        MarshallerSchemaBuilder addValue(String name, BinaryMode binaryMode) {
            int schemaIndex = fieldOrder.indexOf(name);
            values.add(new MarshallerColumn(schemaIndex, name.toUpperCase(Locale.US), binaryMode, null, 0));
            return this;
        }

        TestMarshallerSchema build() {
            MarshallerSchema schema = new MarshallerSchema() {
                @Override
                public int schemaVersion() {
                    return version;
                }

                @Override
                public MarshallerColumn[] keys() {
                    return keys.toArray(MarshallerColumn[]::new);
                }

                @Override
                public MarshallerColumn[] values() {
                    return values.toArray(MarshallerColumn[]::new);
                }

                @Override
                public MarshallerColumn[] row() {
                    return Stream.concat(keys.stream(), values.stream()).toArray(MarshallerColumn[]::new);
                }
            };

            return new TestMarshallerSchema(schema, schema.row());
        }
    }
}
