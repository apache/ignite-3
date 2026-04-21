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

package org.apache.ignite.internal.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.ignite.internal.binarytuple.BinaryTuple;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the {@link BinaryRowConverter} class. */
public class BinaryRowConverterTest extends BaseIgniteAbstractTest {

    private final Random random = new Random();

    @BeforeEach
    public void setup() {
        long seed = System.nanoTime();
        random.setSeed(seed);

        log.info("Seed: {}", seed);
    }

    @Test
    public void testColumnExtractor() {
        List<Column> columnList = Arrays.asList(
                new Column("C1".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("C2".toUpperCase(Locale.ROOT), NativeTypes.INT64, false),
                new Column("C3".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
                new Column("C4".toUpperCase(Locale.ROOT), NativeTypes.DATE, false)
        );
        SchemaDescriptor schema = new SchemaDescriptor(1, columnList, List.of("C2", "C4"), null);

        int col1 = random.nextInt();
        long col2 = random.nextLong();
        String col3 = String.valueOf(random.nextInt());
        LocalDate col4 = LocalDate.ofEpochDay(random.nextInt(10000));

        ByteBuffer builder = new BinaryTupleBuilder(4, 128)
                .appendInt(col1)
                .appendLong(col2)
                .appendString(col3)
                .appendDate(col4)
                .build();

        BinaryRow binaryRow = new BinaryRowImpl(schema.version(), builder);

        {
            BinaryRowConverter columnsExtractor = BinaryRowConverter.columnsExtractor(schema, 0, 1);
            BinaryTuple tuple = columnsExtractor.extractColumns(binaryRow);
            BinaryTupleSchema dstSchema = columnsExtractor.dstSchema();

            assertEquals(col1, dstSchema.value(tuple, 0));
            assertEquals(col2, dstSchema.value(tuple, 1));
            assertTrue(columnsExtractor.columnsMatch(binaryRow, tuple));
        }

        {
            BinaryRowConverter columnsExtractor = BinaryRowConverter.columnsExtractor(schema, 1, 2);
            BinaryTupleSchema dstSchema = columnsExtractor.dstSchema();

            BinaryTuple tuple = columnsExtractor.extractColumns(binaryRow);
            assertEquals(col2, dstSchema.value(tuple, 0));
            assertEquals(col3, dstSchema.value(tuple, 1));
            assertTrue(columnsExtractor.columnsMatch(binaryRow, tuple));
        }

        {
            BinaryRowConverter columnsExtractor = BinaryRowConverter.columnsExtractor(schema, 1, 3);
            BinaryTupleSchema dstSchema = columnsExtractor.dstSchema();

            BinaryTuple tuple = columnsExtractor.extractColumns(binaryRow);
            assertEquals(col2, dstSchema.value(tuple, 0));
            assertEquals(col4, dstSchema.value(tuple, 1));
            assertTrue(columnsExtractor.columnsMatch(binaryRow, tuple));
        }
    }

    @Test
    public void columnsMatchReturnsTrueWhenExtractedTupleIsUsed() {
        // For any row, columnsMatch(row, extractColumns(row)) must be true —
        // this is the fundamental contract the method is built on.
        List<Column> columnList = Arrays.asList(
                new Column("C1".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("C2".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
                new Column("C3".toUpperCase(Locale.ROOT), NativeTypes.INT64, false)
        );
        SchemaDescriptor schema = new SchemaDescriptor(1, columnList, List.of("C1"), null);

        int col1 = random.nextInt();
        String col2 = "hello-" + random.nextInt();
        long col3 = random.nextLong();

        ByteBuffer buffer = new BinaryTupleBuilder(3, 64)
                .appendInt(col1)
                .appendString(col2)
                .appendLong(col3)
                .build();
        BinaryRow row = new BinaryRowImpl(schema.version(), buffer);

        // Single column subset.
        BinaryRowConverter singleCol = BinaryRowConverter.columnsExtractor(schema, 1);
        assertTrue(singleCol.columnsMatch(row, singleCol.extractColumns(row)));

        // Two-column subset.
        BinaryRowConverter twoCol = BinaryRowConverter.columnsExtractor(schema, 0, 2);
        assertTrue(twoCol.columnsMatch(row, twoCol.extractColumns(row)));

        // All columns.
        BinaryRowConverter allCols = BinaryRowConverter.columnsExtractor(schema, 0, 1, 2);
        assertTrue(allCols.columnsMatch(row, allCols.extractColumns(row)));
    }

    @Test
    public void columnsMatchReturnsFalseWhenValueDiffers() {
        List<Column> columnList = Arrays.asList(
                new Column("C1".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("C2".toUpperCase(Locale.ROOT), NativeTypes.INT64, false),
                new Column("C3".toUpperCase(Locale.ROOT), NativeTypes.STRING, false)
        );
        SchemaDescriptor schema = new SchemaDescriptor(1, columnList, List.of("C1"), null);

        int col1 = 42;
        long col2 = 100L;
        String col3 = "abc";

        ByteBuffer buffer = new BinaryTupleBuilder(3, 32)
                .appendInt(col1)
                .appendLong(col2)
                .appendString(col3)
                .build();
        BinaryRow row = new BinaryRowImpl(schema.version(), buffer);

        // Build a tuple where C1 has a different integer value.
        BinaryRowConverter extractor = BinaryRowConverter.columnsExtractor(schema, 0);
        BinaryTuple differentInt = new BinaryTuple(1,
                new BinaryTupleBuilder(1).appendInt(col1 + 1).build());
        assertFalse(extractor.columnsMatch(row, differentInt));

        // Build a tuple where C2 has a different long value.
        BinaryRowConverter extractor2 = BinaryRowConverter.columnsExtractor(schema, 1);
        BinaryTuple differentLong = new BinaryTuple(1,
                new BinaryTupleBuilder(1).appendLong(col2 + 1).build());
        assertFalse(extractor2.columnsMatch(row, differentLong));

        // Build a tuple where C3 has a different string value.
        BinaryRowConverter extractor3 = BinaryRowConverter.columnsExtractor(schema, 2);
        BinaryTuple differentStr = new BinaryTuple(1,
                new BinaryTupleBuilder(1).appendString(col3 + "x").build());
        assertFalse(extractor3.columnsMatch(row, differentStr));
    }

    @Test
    public void columnsMatchHandlesNullValues() {
        List<Column> columnList = Arrays.asList(
                new Column("ID".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("VAL".toUpperCase(Locale.ROOT), NativeTypes.INT32, true)
        );
        SchemaDescriptor schema = new SchemaDescriptor(1, columnList, List.of("ID"), null);

        // Row where both columns are null.
        ByteBuffer buffer = new BinaryTupleBuilder(2)
                .appendInt(1)
                .appendNull()
                .build();
        BinaryRow row = new BinaryRowImpl(schema.version(), buffer);

        // A tuple also carrying null for the same column → match.
        BinaryRowConverter extractor = BinaryRowConverter.columnsExtractor(schema, 1);
        BinaryTuple nullTuple = new BinaryTuple(1, new BinaryTupleBuilder(1).appendNull().build());
        assertTrue(extractor.columnsMatch(row, nullTuple));

        // A tuple carrying a non-null value → no match.
        BinaryTuple nonNullTuple = new BinaryTuple(1,
                new BinaryTupleBuilder(1).appendInt(0).build());
        assertFalse(extractor.columnsMatch(row, nonNullTuple));

        // Row with a non-null value, tuple with null → no match.
        ByteBuffer buffer2 = new BinaryTupleBuilder(2, 16)
                .appendInt(7)
                .appendString("hello")
                .build();
        BinaryRow nonNullRow = new BinaryRowImpl(schema.version(), buffer2);
        assertFalse(extractor.columnsMatch(nonNullRow, nullTuple));
    }

    @Test
    public void columnsMatchAndExtractColumnsAreConsistent() {
        // Verify that for any randomly generated row, the result of extractColumns
        // always satisfies columnsMatch — i.e., the two methods agree on equality.
        List<Column> columnList = Arrays.asList(
                new Column("C1".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("C2".toUpperCase(Locale.ROOT), NativeTypes.INT64, false),
                new Column("C3".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
                new Column("C4".toUpperCase(Locale.ROOT), NativeTypes.DATE, false)
        );
        SchemaDescriptor schema = new SchemaDescriptor(1, columnList, List.of("C1"), null);
        BinaryRowConverter extractor = BinaryRowConverter.columnsExtractor(schema, 1, 3);

        for (int i = 0; i < 20; i++) {
            ByteBuffer buffer = new BinaryTupleBuilder(4, 64)
                    .appendInt(random.nextInt())
                    .appendLong(random.nextLong())
                    .appendString(String.valueOf(random.nextInt()))
                    .appendDate(LocalDate.ofEpochDay(random.nextInt(10000)))
                    .build();
            BinaryRow row = new BinaryRowImpl(schema.version(), buffer);

            BinaryTuple extracted = extractor.extractColumns(row);
            assertTrue(extractor.columnsMatch(row, extracted),
                    "columnsMatch must return true for a tuple produced by extractColumns");
        }
    }

    @Test
    public void testKeyExtractor() {
        List<Column> columnList = Arrays.asList(
                new Column("C1".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("C2".toUpperCase(Locale.ROOT), NativeTypes.INT64, false),
                new Column("C3".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
                new Column("C4".toUpperCase(Locale.ROOT), NativeTypes.DATE, false)
        );
        SchemaDescriptor schema = new SchemaDescriptor(1, columnList, List.of("C2", "C4"), null);

        int col1 = random.nextInt();
        long col2 = random.nextLong();
        String col3 = String.valueOf(random.nextInt());
        LocalDate col4 = LocalDate.ofEpochDay(random.nextInt(10000));

        ByteBuffer builder = new BinaryTupleBuilder(4, 128)
                .appendInt(col1)
                .appendLong(col2)
                .appendString(col3)
                .appendDate(col4)
                .build();

        BinaryRow binaryRow = new BinaryRowImpl(schema.version(), builder);

        {
            BinaryRowConverter columnsExtractor = BinaryRowConverter.keyExtractor(schema);
            BinaryTupleSchema dstSchema = columnsExtractor.dstSchema();

            BinaryTuple tuple = columnsExtractor.extractColumns(binaryRow);
            assertEquals(col2, dstSchema.value(tuple, 0));
            assertEquals(col4, dstSchema.value(tuple, 1));
        }
    }
}
