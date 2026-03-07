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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningColumns;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadataExtractor;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.SourceAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
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
                .distribution(TestBuilders.affinity(List.of(0), 1, 2))
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
    public void testTableScanWithRequiredColumns() throws Exception {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(TestBuilders.affinity(List.of(0), 1, 2))
                .build();

        PartitionPruningMetadata actual = extractMetadata(
                "SELECT c2 FROM t WHERE c2 = 42",
                table
        );

        // Should return null, since c2 is not a colocation key.
        PartitionPruningColumns cols = actual.get(1);
        assertNull(cols);
    }

    @Test
    public void testIndexScan() throws Exception {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(TestBuilders.affinity(List.of(0), 1, 2))
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
    public void testIndexScanWithRequiredColumns() throws Exception {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(TestBuilders.affinity(List.of(0), 1, 2))
                .sortedIndex().name("C1_SORTED")
                .addColumn("C1", Collation.ASC_NULLS_FIRST)
                .end()
                .build();

        PartitionPruningMetadata actual = extractMetadata(
                "SELECT c2 FROM t WHERE c2 = 42",
                table
        );

        // Should return null, since c2 is not a colocation key.
        PartitionPruningColumns cols = actual.get(1);
        assertNull(cols);
    }

    @Test
    public void testMultipleScans() throws Exception {
        IgniteTable table1 = TestBuilders.table()
                .name("T1")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(TestBuilders.affinity(List.of(0), 1, 2))
                .build();

        IgniteTable table2 = TestBuilders.table()
                .name("T2")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(TestBuilders.affinity(List.of(0), 1, 2))
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

    @Test
    public void testExtractorIsReusable() throws Exception {
        IgniteTable table1 = TestBuilders.table()
                .name("T1")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(TestBuilders.affinity(List.of(0), 1, 2))
                .build();

        IgniteTable table2 = TestBuilders.table()
                .name("T2")
                .addKeyColumn("C1", NativeTypes.STRING)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(TestBuilders.affinity(List.of(0), 1, 2))
                .build();

        PartitionPruningMetadataExtractor extractor = new PartitionPruningMetadataExtractor();

        PartitionPruningMetadata result1 = extractMetadata(extractor, "SELECT * FROM t1 WHERE c1=42", table1);
        PartitionPruningMetadata result2 = extractMetadata(extractor, "SELECT * FROM t2 WHERE c1='abc'", table2);

        // check results won't mix
        assertEquals(1, result1.data().size(), "result1 size");
        assertEquals(1, result2.data().size(), "result2 size");

        PartitionPruningColumns col1 = result1.get(1);

        RexLiteral lit1 = (RexLiteral) col1.columns().get(0).get(0);
        assertEquals(SqlTypeName.INTEGER, lit1.getType().getSqlTypeName());

        PartitionPruningColumns col2 = result2.get(1);
        RexLiteral lit2 = (RexLiteral) col2.columns().get(0).get(0);
        assertEquals(SqlTypeName.CHAR, lit2.getType().getSqlTypeName());
    }

    @Test
    public void testInsert() throws Exception {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addKeyColumn("C2", NativeTypes.INT32)
                .addColumn("C3", NativeTypes.INT32, true)
                .distribution(TestBuilders.affinity(List.of(1, 0), 1, 2))
                .build();

        PartitionPruningMetadata actual = extractMetadata(
                "INSERT INTO t(C3, C2, C1) VALUES(null, 1, 2), (null, 3, 4)",
                table
        );

        PartitionPruningColumns cols = actual.get(1);
        assertNotNull(cols, "No metadata for source=1");
        assertEquals("[[0=2, 1=1], [0=4, 1=3]]", PartitionPruningColumns.canonicalForm(cols).toString());
    }

    @Test
    public void testInsertFromSelect() throws Exception {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addKeyColumn("C2", NativeTypes.INT32)
                .addColumn("C3", NativeTypes.INT32, true)
                .distribution(TestBuilders.affinity(List.of(1, 0), 1, 2))
                .build();
        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t SELECT * FROM t WHERE c2=42 and c1=?",
                    table
            );
            expectMetadata(Map.of(1L, "[[0=?0, 1=42]]", 2L, "[[0=?0, 1=42]]"), actual);
        }

        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t SELECT c1, c2, c3 FROM t WHERE c2=42 and c1=?",
                    table
            );
            expectMetadata(Map.of(1L, "[[0=?0, 1=42]]", 2L, "[[0=?0, 1=42]]"), actual);
        }

        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t SELECT c3, c1, c2 FROM t WHERE c2=42 and c1=?",
                    table
            );
            expectMetadata(Map.of(), actual);
        }

        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t SELECT ?, 100, c3 FROM t WHERE c1=? and c2=42",
                    table
            );
            // expectMetadata(Map.of(1L, "[[0=?0, 1=100]]", 2L, "[[0=99, 1=42]]"), actual);
            assertTrue(actual.data().isEmpty(), "Columns: " + actual);
        }

        // No pruning data
        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t SELECT * FROM t WHERE c3=99",
                    table
            );
            assertTrue(actual.data().isEmpty(), "Columns: " + actual);
        }
    }

    @Test
    public void insertToSelectKvInsert() throws Exception {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addKeyColumn("C2", NativeTypes.INT32)
                .addColumn("C3", NativeTypes.INT32, true)
                .distribution(TestBuilders.affinity(List.of(1, 0), 1, 2))
                .build();

        {
            String query = "INSERT INTO t SELECT 42 as c1, 99 as c2, 0 as c3";
            PartitionPruningMetadata actual = extractMetadata(
                    query,
                    table
            );
            assertTrue(actual.data().isEmpty(), "Columns: " + actual);

            // Key value plan
            IgniteSchema schema = createSchema(table);
            IgniteRel rel = physicalPlan(query, schema);
            assertInstanceOf(IgniteKeyValueModify.class, rel);
        }
    }

    @Test
    public void testInsertFromSelectTablesDifferentColumnCount() throws Exception {
        IgniteTable table1 = TestBuilders.table()
                .name("T1")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addKeyColumn("C2", NativeTypes.INT32)
                .addColumn("C3", NativeTypes.INT32, true)
                .addColumn("C4", NativeTypes.INT32, true)
                .distribution(TestBuilders.affinity(List.of(1, 0), 1, 2))
                .build();

        IgniteTable table2 = TestBuilders.table()
                .name("T2")
                .addKeyColumn("K1", NativeTypes.INT32)
                .addKeyColumn("K2", NativeTypes.INT32)
                .addColumn("K3", NativeTypes.INT32, true)
                .addColumn("K4", NativeTypes.INT32, true)
                .addColumn("K5", NativeTypes.INT32, true)
                .distribution(TestBuilders.affinity(List.of(1, 0), 1, 2))
                .build();

        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t1 SELECT k1, k2, k3, k5 FROM t2 WHERE k2=42 and k1=?",
                    table1, table2
            );

            expectMetadata(Map.of(1L, "[[0=?0, 1=42]]", 2L, "[[0=?0, 1=42]]"), actual);
        }
    }

    @Test
    public void testInsertFromSelect2Tables() throws Exception {
        IgniteTable table1 = TestBuilders.table()
                .name("T1")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addKeyColumn("C2", NativeTypes.INT32)
                .addColumn("C3", NativeTypes.INT32, true)
                .distribution(TestBuilders.affinity(List.of(1, 0), 1, 2))
                .build();

        IgniteTable table2 = TestBuilders.table()
                .name("T2")
                .addKeyColumn("K1", NativeTypes.INT32)
                .addKeyColumn("K2", NativeTypes.INT32)
                .addColumn("K3", NativeTypes.INT32, true)
                .distribution(TestBuilders.affinity(List.of(1, 0), 1, 2))
                .build();

        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t1 SELECT * FROM t2 WHERE k2=42 and k1=?",
                    table1, table2
            );

            assertEquals(LongOpenHashSet.of(1, 2), actual.data().keySet(), "Columns: " + actual);

            PartitionPruningColumns cols1 = actual.get(1);
            assertNotNull(cols1, "No metadata for source=1");
            assertEquals("[[0=?0, 1=42]]", PartitionPruningColumns.canonicalForm(cols1).toString());

            PartitionPruningColumns cols2 = actual.get(2);
            assertNotNull(cols1, "No metadata for source=2");
            assertEquals("[[0=?0, 1=42]]", PartitionPruningColumns.canonicalForm(cols2).toString());
        }

        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t1 SELECT k1, k2, k3 FROM t2 WHERE k2=? and k1=99",
                    table1, table2
            );

            assertEquals(LongOpenHashSet.of(1, 2), actual.data().keySet(), "Columns: " + actual);

            PartitionPruningColumns cols1 = actual.get(1);
            assertNotNull(cols1, "No metadata for source=1");
            assertEquals("[[0=99, 1=?0]]", PartitionPruningColumns.canonicalForm(cols1).toString());

            PartitionPruningColumns cols2 = actual.get(2);
            assertNotNull(cols2, "No metadata for source=1");
            assertEquals("[[0=99, 1=?0]]", PartitionPruningColumns.canonicalForm(cols2).toString());
        }
    }

    @Test
    public void testInsertFromSelect2TablesNotMatchingKeyColumns() throws Exception {
        IgniteTable table1 = TestBuilders.table()
                .name("T1")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addKeyColumn("C2", NativeTypes.INT32)
                .addColumn("C3", NativeTypes.INT32, true)
                .distribution(TestBuilders.affinity(List.of(1, 0), 1, 2))
                .build();

        IgniteTable table2 = TestBuilders.table()
                .name("T2")
                .addColumn("C1", NativeTypes.INT32, true)
                .addKeyColumn("C2", NativeTypes.INT32)
                .addKeyColumn("C3", NativeTypes.INT32)
                .distribution(TestBuilders.affinity(List.of(1, 2), 2, 2))
                .build();

        // PP in SELECT
        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t1 SELECT * FROM t2 WHERE c3=99 and c2=42",
                    table1, table2
            );
            expectMetadata(Map.of(2L, "[[1=42, 2=99]]"), actual);
        }

        // PP in SELECT
        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t1 SELECT * FROM t2 WHERE c1=99 and c3=42 and c2=?",
                    table1, table2
            );
            expectMetadata(Map.of(2L, "[[1=?0, 2=42]]"), actual);
        }

        // No metadata
        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t1 SELECT * FROM t2 WHERE c1=99 and c2=?",
                    table1, table2
            );
            assertTrue(actual.data().isEmpty(), "Columns: " + actual);
        }

        {
            PartitionPruningMetadata actual = extractMetadata(
                    "INSERT INTO t1 SELECT * FROM t2 WHERE c1=891",
                    table1, table2
            );
            assertTrue(actual.data().isEmpty(), "Columns: " + actual);
        }
    }

    @Test
    public void testCorrelatedQuery() throws Exception {
        IgniteTable table1 = TestBuilders.table()
                .name("T1")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32, false)
                .distribution(TestBuilders.affinity(List.of(0), 1, 2))
                .build();

        IgniteTable table2 = TestBuilders.table()
                .name("T2")
                .addKeyColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(TestBuilders.affinity(List.of(0), 1, 2))
                .build();

        PartitionPruningMetadataExtractor extractor = new PartitionPruningMetadataExtractor();

        PartitionPruningMetadata actual = extractMetadata(extractor,
                "SELECT /*+ disable_decorrelation */ * FROM t1 as cor WHERE"
                        + " EXISTS (SELECT 1 FROM t2 WHERE t2.c1 = cor.c1 OR t2.c1=42)", table1, table2);

        PartitionPruningColumns cols = actual.get(2);
        assertNotNull(cols, "No metadata for source=2");
        assertEquals("[[0=$cor1.C1], [0=42]]", PartitionPruningColumns.canonicalForm(cols).toString());
    }

    private PartitionPruningMetadata extractMetadata(String query, IgniteTable... table) throws Exception {
        PartitionPruningMetadataExtractor extractor = new PartitionPruningMetadataExtractor();

        return extractMetadata(extractor, query, table);
    }

    private PartitionPruningMetadata extractMetadata(
            PartitionPruningMetadataExtractor extractor,
            String query,
            IgniteTable... table) throws Exception {

        IgniteSchema schema = createSchema(table);
        IgniteRel rel = physicalPlan(query, schema);

        return extractor.go(rel.accept(new AssignSourceIds()));
    }

    private static class AssignSourceIds extends IgniteRelShuttle {

        private long sourceId = 1;

        @Override
        public IgniteRel visit(IgniteRel rel) {
            if (rel instanceof SourceAwareIgniteRel) {
                SourceAwareIgniteRel s = (SourceAwareIgniteRel) rel;
                return super.visit(s.clone(sourceId++));
            } else {
                return super.visit(rel);
            }
        }
    }

    private static void expectMetadata(Map<Long, String> expected, PartitionPruningMetadata actualMetadata) {
        Map<Long, String> actualMetadataAsStr = actualMetadata.data().long2ObjectEntrySet()
                .stream()
                .map(e -> Map.entry(e.getLongKey(), PartitionPruningColumns.canonicalForm(e.getValue()).toString()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertEquals(expected, actualMetadataAsStr, "Partition pruning metadata");
    }
}
