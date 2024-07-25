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

package org.apache.ignite.internal.schema.registry;

import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.DATE;
import static org.apache.ignite.internal.type.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.type.NativeTypes.FLOAT;
import static org.apache.ignite.internal.type.NativeTypes.INT16;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.datetime;
import static org.apache.ignite.internal.type.NativeTypes.time;
import static org.apache.ignite.internal.type.NativeTypes.timestamp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests row assembling and reading.
 */
public class UpgradingRowAdapterTest {
    public static final String NULL_COLUMN_NAME = "valNullCol";
    /** Random. */
    private Random rnd;

    /**
     * Initialization.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        Loggers.forClass(UpgradingRowAdapterTest.class).info("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    @Test
    public void testUpgradeRowWithVariousColumnTypes() {
        SchemaDescriptor schema = createSchemaDescriptorWithColumnsOfAllTypes();
        SchemaDescriptor schema2 = applyAddingValueColumn(schema, 1, new Column("added", INT8, true));

        var schemaRegistry = new SchemaRegistryImpl(
                v -> v == 1 ? schema : schema2,
                schema2
        );

        List<Object> values = generateRowValues(schema);
        BinaryRow originalBinaryRow = serializeValuesToRow(schema, values);

        Row originalRow = Row.wrapBinaryRow(schema, originalBinaryRow);
        Row resolvedRow = schemaRegistry.resolve(originalBinaryRow, schema2);

        // validate UpgradedRowAdapter methods.
        assertThat("Colocation hash mismatch", resolvedRow.colocationHash(), equalTo(originalRow.colocationHash()));
        assertThat("KeyOnly flag mismatch", resolvedRow.keyOnly(), equalTo(originalRow.keyOnly()));
        assertThat("Unexpected element count", resolvedRow.elementCount(), equalTo(originalRow.elementCount() + 1));

        assertNotNull(resolvedRow.byteBuffer());
        assertNull(resolvedRow.binaryTuple(), "Underlying binary tuple must never be used");
        assertThrows(UnsupportedOperationException.class, resolvedRow::tupleSlice, "Underlying binary tuple must never be used");
        assertThrows(UnsupportedOperationException.class, resolvedRow::tupleSliceLength, "Underlying binary tuple must never be used");

        // Validate original row.
        validateRow(values, originalRow);

        // Validate upgraded row.
        values.add(1, null);
        validateRow(values, resolvedRow);

        BinaryRowImpl restoredRow = new BinaryRowImpl(schema2.version(), resolvedRow.byteBuffer());
        assertThat(restoredRow.schemaVersion(), equalTo(schema2.version()));
        validateRow(values, Row.wrapBinaryRow(schema2, restoredRow));
    }

    private static SchemaDescriptor createSchemaDescriptorWithColumnsOfAllTypes() {
        return new SchemaDescriptor(1,
                List.of(new Column("valBooleanCol", BOOLEAN, true),
                        new Column("valByteCol", INT8, true),
                        new Column("valShortCol", INT16, true),
                        new Column("valIntCol", INT32, true),
                        new Column("keyUuidCol", NativeTypes.UUID, false),
                        new Column("valLongCol", INT64, true),
                        new Column(NULL_COLUMN_NAME, INT64, true),
                        new Column("valFloatCol", FLOAT, true),
                        new Column("valDoubleCol", DOUBLE, true),
                        new Column("valDateCol", DATE, true),
                        new Column("valTimeCol", time(0), true),
                        new Column("valDateTimeCol", datetime(6), true),
                        new Column("valTimeStampCol", timestamp(6), true),
                        new Column("valBytesCol", BYTES, false),
                        new Column("valStringCol", STRING, false),
                        new Column("valDecimalCol", NativeTypes.decimalOf(25, 5), false)),
                List.of("keyUuidCol"),
                null
        );
    }

    private static SchemaDescriptor applyAddingValueColumn(SchemaDescriptor desc, int position, Column newColumn) {
        List<Column> columns = new ArrayList<>(desc.columns());
        columns.add(position, newColumn);

        SchemaDescriptor newSchema = new SchemaDescriptor(
                desc.version() + 1,
                columns,
                desc.keyColumns().stream().map(Column::name).collect(Collectors.toList()),
                desc.colocationColumns().stream().map(Column::name).collect(Collectors.toList())
        );

        int addedColumnIndex = newSchema.column(newColumn.name()).positionInRow();

        newSchema.columnMapping(new ColumnMapper() {
            @Override
            public ColumnMapper add(Column col) {
                return fail();
            }

            @Override
            public ColumnMapper add(int from, int to) {
                return fail();
            }

            @Override
            public int map(int idx) {
                return idx < addedColumnIndex ? idx : idx == addedColumnIndex ? -1 : idx - 1;
            }

            @Override
            public Column mappedColumn(int idx) {
                return idx == addedColumnIndex ? newSchema.column(idx) : null;
            }
        });

        return newSchema;
    }

    private static void validateRow(List<Object> values, Row row) {
        SchemaDescriptor schema = row.schema();

        for (int i = 0; i < values.size(); i++) {
            Column col = schema.column(i);

            assertThat("Failed for column: " + col, row.hasNullValue(col.positionInRow()), is(equalTo(values.get(i) == null)));
            assertThat("Failed for column: " + col, row.value(col.positionInRow()), is(equalTo(values.get(i))));
        }
    }

    /**
     * Generate row values for given row schema.
     *
     * @param schema Row schema.
     * @return Row values.
     */
    private List<Object> generateRowValues(SchemaDescriptor schema) {
        ArrayList<Object> res = new ArrayList<>(schema.length());

        for (int i = 0; i < schema.length(); i++) {
            NativeType type = schema.column(i).type();

            if (NULL_COLUMN_NAME.equals(schema.column(i).name())) {
                res.add(null);
            } else {
                res.add(SchemaTestUtils.generateRandomValue(rnd, type));
            }
        }

        return res;
    }

    /**
     * Validates row values after serialization-then-deserialization.
     *
     * @param schema Row schema.
     * @param vals Row values.
     * @return Row bytes.
     */
    private static BinaryRow serializeValuesToRow(SchemaDescriptor schema, List<Object> vals) {
        assertEquals(schema.columns().size(), vals.size());

        RowAssembler asm = new RowAssembler(schema, -1);

        for (Object val : vals) {
            asm.appendValue(val);
        }

        return asm.build();
    }
}
