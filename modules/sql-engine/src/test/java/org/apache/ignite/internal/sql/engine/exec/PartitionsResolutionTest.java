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

package org.apache.ignite.internal.sql.engine.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest.TestTableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
import org.junit.jupiter.api.Test;

/** Assignments resolution test. */
public class PartitionsResolutionTest {
    @Test
    public void partitionsResolver() {
        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> factory = rowHandler.factory(rowSchema);
        RowWrapper row = factory.create("1", 100, 200, 100);

        int part1 = getPartition(row, rowHandler, List.of(0, 1));
        int part2 = getPartition(row, rowHandler, List.of(0, 2));
        int part3 = getPartition(row, rowHandler, List.of(0, 3));

        assertEquals(part1, part3);
        assertNotEquals(part1, part2);
    }

    @Test
    public void rehashingPartitionsResolver() {
        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> factory = rowHandler.factory(rowSchema);
        RowWrapper row = factory.create("1", 100, 200, 100);
        int[] keys1 = {0, 1};
        int[] keys2 = {0, 2};
        int[] keys3 = {0, 3};

        var resolver1 = new RehashingPartitionExtractor<>(1000, keys1, rowHandler);
        var resolver2 = new RehashingPartitionExtractor<>(1000, keys2, rowHandler);
        var resolver3 = new RehashingPartitionExtractor<>(1000, keys3, rowHandler);

        int part1 = resolver1.partition(row);
        int part2 = resolver2.partition(row);
        int part3 = resolver3.partition(row);

        assertEquals(part1, part3);
        assertEquals(part1, resolver1.partition(row));
        assertNotEquals(part1, part2);
    }

    private static int getPartition(RowWrapper row, RowHandler<RowWrapper> rowHandler, List<Integer> distrKeys) {
        TableDescriptor desc = createTableDescriptor(distrKeys);

        int[] colocationColumns = desc.distribution().getKeys().toIntArray();

        NativeType[] fieldTypes = new NativeType[colocationColumns.length];

        for (int i = 0; i < colocationColumns.length; ++i) {
            ColumnDescriptor colDesc = desc.columnDescriptor(i);

            fieldTypes[i] = colDesc.physicalType();
        }

        PartitionCalculator calc = new PartitionCalculator(100, fieldTypes);
        TablePartitionExtractor<RowWrapper> extractor = new TablePartitionExtractor<>(calc, colocationColumns, desc, rowHandler);
        return extractor.partition(row);
    }

    private final StructNativeType rowSchema = NativeTypes.rowBuilder()
            .addField("C1", NativeTypes.STRING, true)
            .addField("C2", NativeTypes.INT32, true)
            .addField("C3", NativeTypes.INT32, true)
            .addField("C4", NativeTypes.INT32, true)
            .build();

    private static TableDescriptor createTableDescriptor(List<Integer> distrKeys) {
        Builder rowTypeBuilder = new Builder(Commons.typeFactory());

        rowTypeBuilder = rowTypeBuilder.add("col1", SqlTypeName.VARCHAR)
        .add("col2", SqlTypeName.INTEGER)
        .add("col3", SqlTypeName.INTEGER)
        .add("col4", SqlTypeName.INTEGER);

        RelDataType rowType =  rowTypeBuilder.build();

        return new TestTableDescriptor(() -> IgniteDistributions.affinity(distrKeys, 1, 1, "test"), rowType);
    }
}
