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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test checks the correctness of the {@link SchemaDescriptor#columnMapping() mapper} created using
 * the {@link SchemaUtils#columnMapper(SchemaDescriptor, SchemaDescriptor)} method.
 */
public class SchemaColumnMapperTest extends BaseIgniteAbstractTest {
    private static final int TOTAL_ITERATIONS = 30;

    private static final SchemaDescriptor INITIAL_SCHEMA =
            new SchemaDescriptor(0, new Column[]{new Column("ID", NativeTypes.INT32, false)}, new Column[0]);

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void removeFirstColumns(int batchSize) {
        SchemaDescriptor newSchema = INITIAL_SCHEMA;

        // Sequentially add columns and check the mapping for the previous version.
        for (int i = 0; i < TOTAL_ITERATIONS; i += batchSize) {
            SchemaDescriptor oldSchema = newSchema;
            newSchema = addColumns(oldSchema, makeColumns(i, batchSize));

            verifyMapping(oldSchema, newSchema);
        }

        // Sequentially remove columns located at the beginning, according to the column order.
        for (int i = 0; i < TOTAL_ITERATIONS; i += batchSize) {
            SchemaDescriptor oldSchema = newSchema;
            int[] idxs = IntStream.range(0, batchSize).toArray();
            newSchema = removeColumns(newSchema, idxs);

            verifyMapping(oldSchema, newSchema);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void removeLastColumns(int batchSize) {
        SchemaDescriptor newSchema = addColumns(INITIAL_SCHEMA, makeColumns(0, TOTAL_ITERATIONS));

        for (int i = TOTAL_ITERATIONS - 1; i >= 0; i -= batchSize) {
            SchemaDescriptor oldSchema = newSchema;
            int[] idxs = IntStream.range(i, i - batchSize).toArray();
            newSchema = removeColumns(newSchema, idxs);

            verifyMapping(oldSchema, newSchema);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void removeRandomColumns(int batchSize) {
        long seed = System.currentTimeMillis();

        log.info("Using seed: " + seed);

        Random rnd = new Random(seed);

        SchemaDescriptor newSchema = addColumns(INITIAL_SCHEMA, makeColumns(INITIAL_SCHEMA.length(), TOTAL_ITERATIONS));

        for (int i = TOTAL_ITERATIONS - 1; i >= batchSize; i -= batchSize) {
            int[] idxs = rnd.ints(i - batchSize, i).distinct().limit(batchSize).toArray();

            SchemaDescriptor oldSchema = newSchema;
            newSchema = removeColumns(newSchema, idxs);

            verifyMapping(oldSchema, newSchema);
        }
    }

    private static void verifyMapping(SchemaDescriptor oldSchema, SchemaDescriptor newSchema) {
        List<Column> oldCols = oldSchema.columns();
        List<Column> newCols = newSchema.columns();
        ColumnMapper mapper = SchemaUtils.columnMapper(oldSchema, newSchema);

        Map<Integer, Column> schemaIndexMap = oldCols.stream()
                .collect(Collectors.toMap(Column::positionInRow, Function.identity()));

        for (Column column : newCols) {
            int newSchemaIdx = column.positionInRow();
            int oldSchemaIdx = mapper.map(newSchemaIdx);

            Column oldCol = oldSchemaIdx < 0 ? mapper.mappedColumn(newSchemaIdx) : schemaIndexMap.get(oldSchemaIdx);

            assertThat("old=" + oldSchema + ", new=" + newSchema, oldCol, equalTo(column));
        }
    }

    private static SchemaDescriptor addColumns(SchemaDescriptor schema, Column ... newColumns) {
        List<Column> columns = new ArrayList<>(schema.valueColumns());
        columns.addAll(List.of(newColumns));

        return new SchemaDescriptor(schema.version() + 1, schema.keyColumns().toArray(new Column[0]), columns.toArray(new Column[0]));
    }

    private static SchemaDescriptor removeColumns(SchemaDescriptor schema, int... idxs) {
        List<Column> newColumns = new ArrayList<>();
        Set<Integer> colIdxsSet = Arrays.stream(idxs).boxed().collect(Collectors.toSet());

        for (int i = 0; i < schema.valueColumns().size(); i++) {
            if (!colIdxsSet.contains(i)) {
                newColumns.add(schema.valueColumns().get(i));
            }
        }

        return new SchemaDescriptor(schema.version() + 1, schema.keyColumns().toArray(new Column[0]), newColumns.toArray(new Column[0]));
    }

    private static Column[] makeColumns(int offset, int count) {
        Column[] columns = new Column[count];

        for (int i = 0; i < count; i++) {
            int index = offset + i;

            NativeType type = SchemaTestUtils.ALL_TYPES.get((SchemaTestUtils.ALL_TYPES.size() - 1) % (index + 1));

            columns[i] = new Column("COL" + index, type, false);
        }

        return columns;
    }
}
