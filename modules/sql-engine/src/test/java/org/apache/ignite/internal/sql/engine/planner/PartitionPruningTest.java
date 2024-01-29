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

package org.apache.ignite.internal.sql.engine.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningColumns;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadataExtractor;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.SourceAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PartitionPruningMetadataExtractor} against different physical operators.
 */
public class PartitionPruningTest extends AbstractPlannerTest {

    @Test
    public void testTableScan() throws Exception {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(IgniteDistributions.affinity(List.of(0), 1, 2))
                .build();

        PartitionPruningMetadata actual = extractMetadata(
                "SELECT * FROM t WHERE c1 = 42",
                table
        );

        PartitionPruningColumns cols = actual.get(1);
        assertNotNull(cols, "No metadata for source=1");
        assertEquals("[[0=42]]", PartitionPruningColumns.canonicalForm(cols).toString());
    }

    @Test
    public void testIndexScan() throws Exception {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(IgniteDistributions.affinity(List.of(0), 1, 2))
                .sortedIndex().name("C1_SORTED")
                .addColumn("C1", Collation.ASC_NULLS_FIRST)
                .end()
                .build();

        PartitionPruningMetadata actual = extractMetadata(
                "SELECT * FROM t WHERE c1 = 42",
                table
        );

        PartitionPruningColumns cols = actual.get(1);
        assertNotNull(cols, "No metadata for source=1");
        assertEquals("[[0=42]]", PartitionPruningColumns.canonicalForm(cols).toString());
    }

    @Test
    public void testMultipleScans() throws Exception {
        IgniteTable table1 = TestBuilders.table()
                .name("T1")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(IgniteDistributions.affinity(List.of(0), 1, 2))
                .build();

        IgniteTable table2 = TestBuilders.table()
                .name("T2")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(IgniteDistributions.affinity(List.of(0), 1, 2))
                .build();

        PartitionPruningMetadata actual = extractMetadata(
                "SELECT * FROM t1 WHERE c1 = 42 UNION SELECT * FROM t2 WHERE c1 = 99",
                table1, table2
        );

        PartitionPruningColumns cols1 = actual.get(1);
        assertNotNull(cols1, "No metadata for source=1");
        assertEquals("[[0=42]]", PartitionPruningColumns.canonicalForm(cols1).toString());

        PartitionPruningColumns cols2 = actual.get(2);
        assertNotNull(cols2, "No metadata for source=2");
        assertEquals("[[0=99]]", PartitionPruningColumns.canonicalForm(cols2).toString());
    }

    private PartitionPruningMetadata extractMetadata(String query, IgniteTable... table) throws Exception {
        IgniteSchema schema = createSchema(table);

        IgniteRel rel = physicalPlan(query, schema);

        PartitionPruningMetadataExtractor extractor = new PartitionPruningMetadataExtractor();
        return extractor.go(rel.accept(new AssignSourceIds()));
    }

    private static class AssignSourceIds extends IgniteRelShuttle {

        private long sourceId = 1;

        @Override
        public IgniteRel visit(IgniteRel rel) {
            if (rel instanceof SourceAwareIgniteRel) {
                SourceAwareIgniteRel s = (SourceAwareIgniteRel) rel;
                return s.clone(sourceId++);
            } else {
                return super.visit(rel);
            }
        }
    }
}
