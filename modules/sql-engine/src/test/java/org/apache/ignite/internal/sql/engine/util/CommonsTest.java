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

package org.apache.ignite.internal.sql.engine.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

/**
 * Tests for utility functions defined in {@link Commons}.
 */
public class CommonsTest extends BaseIgniteAbstractTest {

    @Test
    public void testTrimmingMapping() {
        ImmutableBitSet requiredElements = ImmutableBitSet.of(1, 4, 3, 7);

        Mapping mapping = Commons.trimmingMapping(requiredElements.length(), requiredElements);
        expectMapped(mapping, ImmutableBitSet.of(1, 7), ImmutableBitSet.of(0, 3));
        expectMapped(mapping, ImmutableBitSet.of(3), ImmutableBitSet.of(1));
        expectMapped(mapping, ImmutableBitSet.of(1, 4, 3, 7), ImmutableBitSet.of(0, 1, 2, 3));
    }

    @Test
    public void testProjectionMapping() {
        assertProjectionMapping(
                ImmutableIntList.of(1, 2, 3, 4), // source
                ImmutableIntList.of(3, 2), // projection
                ImmutableIntList.of(4, 3) // expected result
        );

        assertProjectionMapping(
                ImmutableIntList.of(1, 2, 3, 4), // source
                ImmutableIntList.of(3, 0), // projection
                ImmutableIntList.of(4, 1) // expected result
        );

        assertProjectionMapping(
                ImmutableIntList.of(1, 2, 3, 4), // source
                ImmutableIntList.of(1), // projection
                ImmutableIntList.of(2) // expected result
        );
    }

    @Test
    public void testArrayToMap() {
        assertEquals(Map.of(), Commons.arrayToMap(null));

        HashMap<Integer, Object> vals = new HashMap<>();
        vals.put(0, 1);
        vals.put(1, null);
        vals.put(2, "3");
        assertEquals(vals, Commons.arrayToMap(new Object[]{1, null, "3"}));
    }

    @Test
    public void testCastInputsToLeastRestrictiveTypeIfNeeded() {
        IgniteTypeFactory tf = Commons.typeFactory();

        RelDataType row1 = new RelDataTypeFactory.Builder(tf)
                .add("C1", tf.createSqlType(SqlTypeName.INTEGER))
                .add("C2", tf.createSqlType(SqlTypeName.INTEGER))
                .build();

        RelDataType row2 = new RelDataTypeFactory.Builder(tf)
                .add("C1", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.INTEGER), true))
                .add("C2", tf.createSqlType(SqlTypeName.REAL))
                .build();

        RelDataType row3 = new RelDataTypeFactory.Builder(tf)
                .add("C1", tf.createSqlType(SqlTypeName.INTEGER))
                .add("C2", tf.createSqlType(SqlTypeName.REAL))
                .build();

        RelDataType row4 = new RelDataTypeFactory.Builder(tf)
                .add("C1", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.INTEGER), true))
                .add("C2", tf.createSqlType(SqlTypeName.INTEGER))
                .build();

        RelNode node1 = Mockito.mock(RelNode.class);
        when(node1.getRowType()).thenReturn(row1);

        RelNode node2 = Mockito.mock(RelNode.class);
        when(node2.getRowType()).thenReturn(row2);

        RelNode node3 = Mockito.mock(RelNode.class);
        when(node3.getRowType()).thenReturn(row3);

        RelNode node4 = Mockito.mock(RelNode.class);
        when(node4.getRowType()).thenReturn(row4);

        List<RelNode> relNodes = Commons.castInputsToLeastRestrictiveTypeIfNeeded(List.of(node1, node2, node3, node4), Commons.cluster(),
                Commons.cluster().traitSet());

        RelDataType lt = tf.leastRestrictive(List.of(row1, row2, row3, row4));

        IgniteProject project1 = assertInstanceOf(IgniteProject.class, relNodes.get(0), "node1");
        assertEquals(lt, project1.getRowType(), "Invalid types in projection for node1");

        // Node 2 has the same type as leastRestrictive(row1, row2)
        assertSame(node2, relNodes.get(1), "Invalid types in projection for node2");

        // Nullability is ignored
        assertSame(node3, relNodes.get(2));

        IgniteProject project4 = assertInstanceOf(IgniteProject.class, relNodes.get(3), "node4");
        assertEquals(lt, project4.getRowType(), "Invalid types in projection for node4");
    }

    @ParameterizedTest
    @CsvSource({
            "3, 3",
            "5, 5",
            "2, 7",
    })
    void targetOffset(int sourceSize, int offset) {
        TargetMapping mapping = Commons.targetOffsetMapping(sourceSize, offset);

        for (int i = 0; i < sourceSize; i++) {
            assertThat(
                    "Source <" + i + "> should be shifted by offset <" + offset + ">",
                    mapping.getTarget(i), is(i + offset)
            );
        }
    }

    private static void expectMapped(Mapping mapping, ImmutableBitSet bitSet, ImmutableBitSet expected) {
        assertEquals(expected, Mappings.apply(mapping, bitSet), "direct mapping");

        Mapping inverseMapping = mapping.inverse();
        assertEquals(bitSet, Mappings.apply(inverseMapping, expected), "inverse mapping");
    }

    private static void assertProjectionMapping(ImmutableIntList source, ImmutableIntList projection, ImmutableIntList expected) {
        Mapping mapping = Commons.projectedMapping(source.size(), projection);

        assertEquals(expected, Mappings.apply(mapping, source));
    }
}
