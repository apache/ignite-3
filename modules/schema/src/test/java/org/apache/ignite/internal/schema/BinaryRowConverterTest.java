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

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
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
        }

        {
            BinaryRowConverter columnsExtractor = BinaryRowConverter.columnsExtractor(schema, 1, 2);
            BinaryTupleSchema dstSchema = columnsExtractor.dstSchema();

            BinaryTuple tuple = columnsExtractor.extractColumns(binaryRow);
            assertEquals(col2, dstSchema.value(tuple, 0));
            assertEquals(col3, dstSchema.value(tuple, 1));
        }

        {
            BinaryRowConverter columnsExtractor = BinaryRowConverter.columnsExtractor(schema, 1, 3);
            BinaryTupleSchema dstSchema = columnsExtractor.dstSchema();

            BinaryTuple tuple = columnsExtractor.extractColumns(binaryRow);
            assertEquals(col2, dstSchema.value(tuple, 0));
            assertEquals(col4, dstSchema.value(tuple, 1));
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
