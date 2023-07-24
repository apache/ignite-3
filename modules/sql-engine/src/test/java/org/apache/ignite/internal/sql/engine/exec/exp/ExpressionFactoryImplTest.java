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

package org.apache.ignite.internal.sql.engine.exec.exp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.internal.sql.engine.metadata.FragmentMapping;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * ExpressionFactoryImpl test.
 */
public class ExpressionFactoryImplTest extends BaseIgniteAbstractTest {
    /** Type factory. */
    private IgniteTypeFactory typeFactory;

    /** Expression factory. */
    private ExpressionFactoryImpl<Object[]> expFactory;

    @BeforeEach
    public void prepare() {
        typeFactory = Commons.typeFactory();

        FragmentDescription fragmentDescription = new FragmentDescription(1, true,
                FragmentMapping.create(1), ColocationGroup.forSourceId(1), Long2ObjectMaps.EMPTY_MAP);

        ExecutionContext<Object[]> ctx = TestBuilders.executionContext()
                .queryId(UUID.randomUUID())
                .localNode(new ClusterNodeImpl("1", "node-1", new NetworkAddress("localhost", 1234)))
                .fragment(fragmentDescription)
                .executor(Mockito.mock(QueryTaskExecutor.class))
                .build();

        expFactory = new ExpressionFactoryImpl<>(ctx, typeFactory, SqlConformanceEnum.DEFAULT);
    }

    @Test
    public void testScalarGeneration() {
        RelDataTypeField field = new RelDataTypeFieldImpl(
                "ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)
        );
        RelRecordType type = new RelRecordType(Collections.singletonList(field));

        //Imagine we have 2 columns: (id: INTEGER, val: VARCHAR)
        RexDynamicParam firstNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexDynamicParam secondNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);

        SingleScalar scalar1 = expFactory.scalar(Arrays.asList(firstNode, secondNode), type);

        //Imagine we have 2 columns: (id: VARCHAR, val: INTEGER)
        firstNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        secondNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);

        SingleScalar scalar2 = expFactory.scalar(Arrays.asList(firstNode, secondNode), type);

        assertNotSame(scalar1, scalar2);
    }

    /**
     * Column with {@link IgniteSqlOperatorTable#NULL_BOUND} is converted to {@code null}.
     */
    @Test
    public void testNullBoundIsConvertedToNull() {
        RexBuilder rexBuilder = Commons.rexBuilder();

        // condition expression is not used
        RexLiteral condition = rexBuilder.makeLiteral(true);

        RexNode intValue1 = rexBuilder.makeExactLiteral(new BigDecimal("1"));
        RexNode intValue2 = rexBuilder.makeExactLiteral(new BigDecimal("2"));
        RexNode nullBound = rexBuilder.makeCall(IgniteSqlOperatorTable.NULL_BOUND);

        List<SearchBounds> boundsList = List.of(
                new MultiBounds(condition, List.of(
                        new ExactBounds(condition, intValue1),
                        new ExactBounds(condition, nullBound))
                ),
                new ExactBounds(condition, intValue2)
        );

        RelDataType rowType = new Builder(typeFactory)
                .add("C1", SqlTypeName.INTEGER)
                .add("C2", SqlTypeName.INTEGER)
                .build();

        RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, null);
        List<TestRange> list = new ArrayList<>();

        ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

        List<TestRange> expected = List.of(
                new TestRange(new Object[]{1, 2}),
                new TestRange(new Object[]{null, 2}));

        assertEquals(expected, list);
    }

    /**
     * {@link RangeCondition} with {@code NULL} bound is ignored.
     */
    @Test
    public void testRangeConditionWithNullIsIgnored() {
        RexBuilder rexBuilder = Commons.rexBuilder();

        // condition expression is not used
        RexLiteral condition = rexBuilder.makeLiteral(true);

        RexNode intValue = rexBuilder.makeExactLiteral(new BigDecimal("1"));
        RexNode nullValue = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER));

        List<SearchBounds> boundsList = List.of(
                new MultiBounds(condition, List.of(
                        new ExactBounds(condition, intValue),
                        new ExactBounds(condition, nullValue))
                )
        );

        RelDataType rowType = new Builder(typeFactory)
                .add("C1", SqlTypeName.INTEGER)
                .build();

        RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, null);
        List<TestRange> list = new ArrayList<>();

        ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

        assertEquals(List.of(new TestRange(new Object[]{1})), list);
    }

    static final class TestRange {

        final Object[] lower;

        final Object[] upper;

        TestRange(Object[] lower) {
            this(lower, lower);
        }

        TestRange(Object[] lower, @Nullable Object[] upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestRange testRange = (TestRange) o;
            return Arrays.equals(lower, testRange.lower) && Arrays.equals(upper, testRange.upper);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(lower);
            result = 31 * result + Arrays.hashCode(upper);
            return result;
        }

        @Override
        public String toString() {
            return "{lower=" + Arrays.toString(lower)
                    + ", upper=" + Arrays.toString(upper)
                    + '}';
        }
    }
}
