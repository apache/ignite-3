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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest.TestTableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/** Partition resolution test. */
class AssignmentsResolutionTest {
    @Test
    public void assignmentsResolver() {
        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        int[] colocationKeys = {0, 2};
        RowPartitionExtractorImpl<RowWrapper> rowAssignments =
                new RowPartitionExtractorImpl<>(100, colocationKeys, rowHandler);
        AssignmentExtractorImpl assignments = new AssignmentExtractorImpl(100, colocationKeys.length);

        RowFactory<RowWrapper> factory = rowHandler.factory(rowSchema);
        RowWrapper row = factory.create("1", 1, 2);

        int assignment1 = rowAssignments.getPartition(row);
        assignments.append("1");
        assignments.append(2);
        int assignment2 = assignments.getAssignment();

        assertEquals(assignment1, assignment2);

        assignments.append("1");
        assignments.append(2);
        assignment2 = assignments.getAssignment();

        assertEquals(assignment1, assignment2);

        assignment1 = rowAssignments.getPartition(row);
        assertEquals(assignment1, assignment2);
    }

    @Test
    public void partitionResolver() {
        TableDescriptor desc = createTableDescriptor();
        RowHandler<RowWrapper> rowHandler = SqlRowHandler.INSTANCE;
        RowFactory<RowWrapper> factory = rowHandler.factory(rowSchema);
        RowWrapper row = factory.create("1", 1, 2);
        int[] colocationKeys = {0, 2};

        TypeAwareObjectPartitionExtractor<RowWrapper> partResolver =
                new TypeAwareObjectPartitionExtractor<>(100, colocationKeys, desc, rowHandler);

        TypeAwareRowPartitionExtractor<RowWrapper> rowPartResolver =
                new TypeAwareRowPartitionExtractor<>(100, colocationKeys, desc, rowHandler);

        int part1 = rowPartResolver.getPartition(row);
        partResolver.append("1");
        partResolver.append(2);
        int part2 = partResolver.getPartition();

        assertEquals(part1, part2);

        partResolver.append("1");
        partResolver.append(2);
        assertThrows(AssertionError.class, () -> partResolver.append(2));
        part2 = partResolver.getPartition();

        assertEquals(part1, part2);

        part1 = rowPartResolver.getPartition(row);
        assertEquals(part1, part2);
    }

    private final RowSchema rowSchema = RowSchema.builder()
            .addField(NativeTypes.STRING)
            .addField(NativeTypes.INT16)
            .addField(NativeTypes.INT32)
            .build();

    private static TableDescriptor createTableDescriptor() {
        Builder rowTypeBuilder = new Builder(Commons.typeFactory());

        rowTypeBuilder = rowTypeBuilder.add("col1", SqlTypeName.VARCHAR)
                .add("col2", SqlTypeName.TINYINT)
                .add("col3", SqlTypeName.INTEGER);

        RelDataType rowType =  rowTypeBuilder.build();

        return new TestTableDescriptor(() -> IgniteDistributions.affinity(List.of(0, 2), 1, 1), rowType);
    }
}
