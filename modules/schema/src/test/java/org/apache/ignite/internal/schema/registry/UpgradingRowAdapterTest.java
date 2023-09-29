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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor.INITIAL_TABLE_VERSION;
import static org.apache.ignite.internal.schema.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DATE;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.datetime;
import static org.apache.ignite.internal.schema.NativeTypes.time;
import static org.apache.ignite.internal.schema.NativeTypes.timestamp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests row assembling and reading.
 */
public class UpgradingRowAdapterTest {
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
    public void testVariousColumnTypes() {
        SchemaDescriptor schema = new SchemaDescriptor(1,
                new Column[]{new Column("keyUuidCol", NativeTypes.UUID, false)},
                new Column[]{
                        new Column("valBooleanCol", BOOLEAN, true),
                        new Column("valByteCol", INT8, true),
                        new Column("valShortCol", INT16, true),
                        new Column("valIntCol", INT32, true),
                        new Column("valLongCol", INT64, true),
                        new Column("valFloatCol", FLOAT, true),
                        new Column("valDoubleCol", DOUBLE, true),
                        new Column("valDateCol", DATE, true),
                        new Column("valTimeCol", time(), true),
                        new Column("valDateTimeCol", datetime(), true),
                        new Column("valTimeStampCol", timestamp(), true),
                        new Column("valBitmask1Col", NativeTypes.bitmaskOf(22), true),
                        new Column("valBytesCol", BYTES, false),
                        new Column("valStringCol", STRING, false),
                        new Column("valNumberCol", NativeTypes.numberOf(20), false),
                        new Column("valDecimalCol", NativeTypes.decimalOf(25, 5), false),
                }
        );

        SchemaDescriptor schema2 = new SchemaDescriptor(2,
                new Column[]{new Column("keyUuidCol", NativeTypes.UUID, false)},
                new Column[]{
                        new Column("added", INT8, true),
                        new Column("valBooleanCol", BOOLEAN, true),
                        new Column("valByteCol", INT8, true),
                        new Column("valShortCol", INT16, true),
                        new Column("valIntCol", INT32, true),
                        new Column("valLongCol", INT64, true),
                        new Column("valFloatCol", FLOAT, true),
                        new Column("valDoubleCol", DOUBLE, true),
                        new Column("valDateCol", DATE, true),
                        new Column("valTimeCol", time(), true),
                        new Column("valDateTimeCol", datetime(), true),
                        new Column("valTimeStampCol", timestamp(), true),
                        new Column("valBitmask1Col", NativeTypes.bitmaskOf(22), true),
                        new Column("valBytesCol", BYTES, false),
                        new Column("valStringCol", STRING, false),
                        new Column("valNumberCol", NativeTypes.numberOf(20), false),
                        new Column("valDecimalCol", NativeTypes.decimalOf(25, 5), false),
                }
        );

        int addedColumnIndex = schema2.column("added").schemaIndex();

        schema2.columnMapping(new ColumnMapper() {
            @Override
            public ColumnMapper add(Column col) {
                return null;
            }

            @Override
            public ColumnMapper add(int from, int to) {
                return null;
            }

            @Override
            public int map(int idx) {
                return idx < addedColumnIndex ? idx : idx == addedColumnIndex ? -1 : idx - 1;
            }

            @Override
            public Column mappedColumn(int idx) {
                return idx == addedColumnIndex ? schema2.column(idx) : null;
            }
        });

        List<Object> values = generateRowValues(schema);

        BinaryRow row = serializeValuesToRow(schema, values);

        var schemaRegistry = new SchemaRegistryImpl(
                v -> v == 1 ? completedFuture(schema) : completedFuture(schema2),
                () -> completedFuture(INITIAL_TABLE_VERSION),
                schema
        );

        // Validate row.
        validateRow(values, schemaRegistry, row);

        // Validate upgraded row.
        values.add(addedColumnIndex, null);

        var schema2Registry = new SchemaRegistryImpl(
                v -> v == 1 ? completedFuture(schema) : completedFuture(schema2),
                () -> completedFuture(schema2.version()),
                schema2
        );

        validateRow(values, schema2Registry, row);
    }

    private void validateRow(List<Object> values, SchemaRegistryImpl schemaRegistry, BinaryRow binaryRow) {
        Row row = schemaRegistry.resolve(binaryRow);

        SchemaDescriptor schema = row.schema();

        for (int i = 0; i < values.size(); i++) {
            Column col = schema.column(i);

            assertThat("Failed for column: " + col, row.value(col.schemaIndex()), is(equalTo(values.get(i))));
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

            res.add(SchemaTestUtils.generateRandomValue(rnd, type));
        }

        return res;
    }

    /**
     * Validates row values after serialization-then-deserialization.
     *
     * @param schema Row schema.
     * @param vals   Row values.
     * @return Row bytes.
     */
    private static BinaryRow serializeValuesToRow(SchemaDescriptor schema, List<Object> vals) {
        assertEquals(schema.keyColumns().length() + schema.valueColumns().length(), vals.size());

        RowAssembler asm = new RowAssembler(schema);

        for (Object val : vals) {
            asm.appendValue(val);
        }

        return asm.build();
    }
}
