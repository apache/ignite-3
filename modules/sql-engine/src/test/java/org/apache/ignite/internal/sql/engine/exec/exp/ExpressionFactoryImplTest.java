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

import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeName.Limit;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
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
                Long2ObjectMaps.emptyMap(), null, null, null);

        ExecutionContext<Object[]> ctx = TestBuilders.executionContext()
                .queryId(randomUUID())
                .localNode(new ClusterNodeImpl(randomUUID(), "node-1", new NetworkAddress("localhost", 1234)))
                .fragment(fragmentDescription)
                .executor(Mockito.mock(QueryTaskExecutor.class))
                .build();

        expFactory = new ExpressionFactoryImpl<>(ctx, SqlConformanceEnum.DEFAULT);
    }

    @Test
    public void testScalarGeneration() {
        RelDataTypeField field = new RelDataTypeFieldImpl(
                "ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)
        );
        RelRecordType type = new RelRecordType(singletonList(field));

        // Imagine we have 2 columns: (id: INTEGER, val: VARCHAR)
        RexDynamicParam firstNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexDynamicParam secondNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);

        SingleScalar scalar1 = expFactory.scalar(Arrays.asList(firstNode, secondNode), type);

        // Imagine we have 2 columns: (id: VARCHAR, val: INTEGER)
        firstNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        secondNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);

        SingleScalar scalar2 = expFactory.scalar(Arrays.asList(firstNode, secondNode), type);

        assertNotSame(scalar1, scalar2);
    }

    @Test
    void multiBoundConditionAreOrderedCorrectly() {
        RexBuilder rexBuilder = Commons.rexBuilder();

        // condition expression is not used
        RexLiteral condition = rexBuilder.makeLiteral(true);

        RelDataType rowType = new Builder(typeFactory)
                .add("C1", SqlTypeName.INTEGER)
                .build();

        RexNode intValue1 = rexBuilder.makeExactLiteral(new BigDecimal("1"));
        RexNode intValue5 = rexBuilder.makeExactLiteral(new BigDecimal("5"));
        RexNode nullValue = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER));

        { // two non-null value should be ordered according to collation (ASC)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new ExactBounds(condition, intValue1),
                            new ExactBounds(condition, intValue5)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(0));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

            assertEquals(List.of(new TestRange(new Object[]{1}), new TestRange(new Object[]{5})), list);
        }

        { // two non-null value should be ordered according to collation (DESC)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new ExactBounds(condition, intValue1),
                            new ExactBounds(condition, intValue5)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(new RelFieldCollation(0, Direction.DESCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

            assertEquals(List.of(new TestRange(new Object[]{5}), new TestRange(new Object[]{1})), list);
        }

        { // null value should respect collation (NULLS FIRST)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new ExactBounds(condition, intValue1),
                            new ExactBounds(condition, nullValue)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

            assertEquals(List.of(new TestRange(new Object[]{null}), new TestRange(new Object[]{1})), list);
        }

        { // null value should respect collation (NULLS LAST)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new ExactBounds(condition, intValue1),
                            new ExactBounds(condition, nullValue)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.DESCENDING, NullDirection.LAST)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

            assertEquals(List.of(new TestRange(new Object[]{1}), new TestRange(new Object[]{null})), list);
        }

        { // range condition with lower bound should be ordered according to collation (ASC)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new ExactBounds(condition, intValue1),
                            new RangeBounds(condition, intValue5, null, true, true)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(0));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

            assertEquals(List.of(new TestRange(new Object[]{1}), new TestRange(new Object[]{5}, null)), list);
        }

        { // range condition with lower bound should be ordered according to collation (DESC)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new ExactBounds(condition, intValue1),
                            new RangeBounds(condition, nullValue, intValue5, false, false)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(new RelFieldCollation(0, Direction.DESCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

            assertEquals(List.of(new TestRange(new Object[]{null}, new Object[]{5}), new TestRange(new Object[]{1})), list);
        }

        { // range condition with null value as lower bound should respect collation (NULLS FIRST)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new ExactBounds(condition, intValue1),
                            new RangeBounds(condition, null, nullValue, true, true)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

            assertEquals(List.of(new TestRange(null, new Object[]{null}), new TestRange(new Object[]{1})), list);
        }

        { // range condition with null value as lower bound should respect collation (NULLS LAST)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new RangeBounds(condition, nullValue, null, true, true),
                            new ExactBounds(condition, intValue1)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

            assertEquals(List.of(new TestRange(new Object[]{1}), new TestRange(new Object[]{null}, null)), list);
        }

        { // range condition without lower bound should respect collation (NULLS FIRST)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new ExactBounds(condition, intValue1),
                            new RangeBounds(condition, null, intValue5, true, true)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.upper())));

            assertEquals(List.of(new TestRange(null, new Object[]{5}), new TestRange(new Object[]{1})), list);
        }
    }

    @Test
    void multiBoundConditionsAreMergedCorrectly() {
        RexBuilder rexBuilder = Commons.rexBuilder();

        // condition expression is not used
        RexLiteral condition = rexBuilder.makeLiteral(true);

        RelDataType rowType = new Builder(typeFactory)
                .add("C1", SqlTypeName.INTEGER)
                .build();

        RexNode intValue1 = rexBuilder.makeExactLiteral(new BigDecimal("1"));
        RexNode intValue2 = rexBuilder.makeExactLiteral(new BigDecimal("2"));
        RexNode intValue3 = rexBuilder.makeExactLiteral(new BigDecimal("3"));
        RexNode intValue5 = rexBuilder.makeExactLiteral(new BigDecimal("5"));

        { // conditions 'val < 1 or val = 1 or val = 5' can be combined to 'val <= 1 or val = 5' (ASCENDING)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new ExactBounds(condition, intValue5),
                            new ExactBounds(condition, intValue1),
                            new RangeBounds(condition, null, intValue1, true, false)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.ASCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.lowerInclude(), r.upper(), r.upperInclude())));

            assertEquals(List.of(new TestRange(null, new Object[]{1}), new TestRange(new Object[]{5})), list);
        }

        { // conditions 'val < 1 or val = 1 or val = 5' can be combined to 'val <= 1 or val = 5' (DESCENDING)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new RangeBounds(condition, intValue1, null, false, true),
                            new ExactBounds(condition, intValue1),
                            new ExactBounds(condition, intValue5)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.DESCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.lowerInclude(), r.upper(), r.upperInclude())));

            assertEquals(List.of(new TestRange(new Object[]{5}), new TestRange(new Object[]{1}, null)), list);
        }

        { // conditions 'val >= 1 and val <= 5 or val > 1 and val < 5' must be combined to single 'val >= 1 and val <= 5' (ASCENDING)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new RangeBounds(condition, intValue1, intValue5, true, true),
                            new RangeBounds(condition, intValue1, intValue5, false, false)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.ASCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.lowerInclude(), r.upper(), r.upperInclude())));

            assertEquals(List.of(new TestRange(new Object[]{1}, new Object[]{5})), list);
        }

        { // conditions 'val >= 1 and val <= 5 or val > 1 and val < 5' must be combined to single 'val >= 1 and val <= 5' (DESCENDING)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new RangeBounds(condition, intValue5, intValue1, true, true),
                            new RangeBounds(condition, intValue5, intValue1, false, false)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.DESCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.lowerInclude(), r.upper(), r.upperInclude())));

            assertEquals(List.of(new TestRange(new Object[]{5}, new Object[]{1})), list);
        }

        { // conditions 'val >= 1 and val <= 2 or val >= 2 and val < 5' must be combined to single 'val >= 1 and val < 5' (ASCENDING)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new RangeBounds(condition, intValue1, intValue2, true, true),
                            new RangeBounds(condition, intValue2, intValue5, true, false)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.ASCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.lowerInclude(), r.upper(), r.upperInclude())));

            assertEquals(List.of(new TestRange(new Object[]{1}, true, new Object[]{5}, false)), list);
        }

        { // conditions 'val >= 1 and val <= 2 or val >= 2 and val < 5' must be combined to single 'val >= 1 and val < 5' (DESCENDING)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new RangeBounds(condition, intValue2, intValue1, true, true),
                            new RangeBounds(condition, intValue5, intValue2, false, true)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.DESCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.lowerInclude(), r.upper(), r.upperInclude())));

            assertEquals(List.of(new TestRange(new Object[]{5}, false, new Object[]{1}, true)), list);
        }

        { // conditions 'val >= 1 and val < 3 or val > 2 and val < 5' must be combined into single 'val >= 1 and val <= 5' (ASCENDING)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new RangeBounds(condition, intValue1, intValue3, true, false),
                            new RangeBounds(condition, intValue2, intValue5, false, false)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.ASCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.lowerInclude(), r.upper(), r.upperInclude())));

            assertEquals(List.of(new TestRange(new Object[]{1}, true, new Object[]{5}, false)), list);
        }

        { // conditions 'val >= 1 and val < 3 or val > 2 and val < 5' must be combined into single 'val >= 1 and val <= 5' (DESCENDING)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new RangeBounds(condition, intValue3, intValue1, false, true),
                            new RangeBounds(condition, intValue5, intValue2, false, false)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.DESCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.lowerInclude(), r.upper(), r.upperInclude())));

            assertEquals(List.of(new TestRange(new Object[]{5}, false, new Object[]{1}, true)), list);
        }
    }

    @Test
    public void testInvalidConditions() {
        // At the moment, such conditions are impossible to obtain, but we should be aware of them,
        // since they can break the merge procedure.
        RexBuilder rexBuilder = Commons.rexBuilder();

        // condition expression is not used
        RexLiteral condition = rexBuilder.makeLiteral(true);

        RelDataType rowType = new Builder(typeFactory)
                .add("C1", SqlTypeName.INTEGER)
                .build();

        RexNode intValue1 = rexBuilder.makeExactLiteral(new BigDecimal("1"));
        RexNode intValue2 = rexBuilder.makeExactLiteral(new BigDecimal("2"));

        { // conditions 'val between 2 and 1 or val = 2' should lead to single 'val = 2' (ASCENDING)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new RangeBounds(condition, intValue2, intValue1, true, true),
                            new ExactBounds(condition, intValue2)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.ASCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.lowerInclude(), r.upper(), r.upperInclude())));

            assertEquals(List.of(new TestRange(new Object[]{2})), list);
        }

        { // conditions 'val between 2 and 1 or val = 2' should lead to single 'val = 2' (DESCENDING)
            List<SearchBounds> boundsList = List.of(
                    new MultiBounds(condition, List.of(
                            new RangeBounds(condition, intValue1, intValue2, true, true),
                            new ExactBounds(condition, intValue2)
                    ))
            );

            Comparator<Object[]> comparator = expFactory.comparator(RelCollations.of(
                    new RelFieldCollation(0, Direction.DESCENDING)));

            RangeIterable<Object[]> ranges = expFactory.ranges(boundsList, rowType, comparator);
            List<TestRange> list = new ArrayList<>();

            ranges.forEach(r -> list.add(new TestRange(r.lower(), r.lowerInclude(), r.upper(), r.upperInclude())));

            assertEquals(List.of(new TestRange(new Object[]{2})), list);
        }
    }

    @ParameterizedTest(name = "condition satisfies the index: [{0}]")
    @ValueSource(booleans = {true, false})
    public void testConditionsNotContainsNulls(boolean conditionSatisfyIdx) {
        RexBuilder rexBuilder = Commons.rexBuilder();

        RexNode val1 = rexBuilder.makeExactLiteral(new BigDecimal("1"));
        RexNode val2 = rexBuilder.makeExactLiteral(new BigDecimal("2"));

        RelDataTypeSystem typeSystem = Commons.emptyCluster().getTypeFactory().getTypeSystem();

        RexLocalRef ref1 = rexBuilder.makeLocalRef(new BasicSqlType(typeSystem, SqlTypeName.INTEGER), conditionSatisfyIdx ? 1 : 3);
        RexLocalRef ref2 = rexBuilder.makeLocalRef(new BasicSqlType(typeSystem, SqlTypeName.INTEGER), 2);

        RexNode pred1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ref1, val1);
        RexNode pred2 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ref2, val2);

        RexNode andCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, pred1, pred2);

        RelDataType rowType = new Builder(typeFactory)
                .add("C1", SqlTypeName.INTEGER)
                .add("C2", SqlTypeName.INTEGER)
                .add("C3", SqlTypeName.INTEGER)
                .add("C4", SqlTypeName.INTEGER)
                .build();

        // build bounds for two sequential columns also belongs to index
        List<SearchBounds> bounds = RexUtils.buildSortedSearchBounds(Commons.emptyCluster(),
                RelCollations.of(ImmutableIntList.of(1, 2)), andCondition, rowType, ImmutableBitSet.of(0, 1, 2));

        if (!conditionSatisfyIdx) {
            assertNull(bounds);
            return;
        } else {
            assertNotNull(bounds);
        }

        RangeIterable<Object[]> ranges = expFactory.ranges(bounds, rowType, null);
        // TODO: https://issues.apache.org/jira/browse/IGNITE-13568 seems length predicate bounds
        //  for sequential columns also belong to index need to be 2
        assertEquals(1, ranges.iterator().next().lower().length);

        // condition expression is not used
        RexLiteral condition = rexBuilder.makeLiteral(true);

        // assembly bounds
        List<SearchBounds> boundsList = List.of(
                new RangeBounds(condition, val1, val2, true, true)
        );

        ranges = expFactory.ranges(boundsList, rowType, null);
        assertEquals(1, ranges.iterator().next().lower().length);
        assertEquals(1, ranges.iterator().next().upper().length);
    }

    @Test
    public void testProject() {
        RexBuilder rexBuilder = Commons.rexBuilder();
        IgniteTypeFactory tf = Commons.typeFactory();

        RelDataType intType = tf.createSqlType(SqlTypeName.INTEGER);
        RelDataType bigIntType = tf.createSqlType(SqlTypeName.BIGINT);

        RexNode val1 = rexBuilder.makeExactLiteral(new BigDecimal("1"), intType);
        RexNode val2 = rexBuilder.makeExactLiteral(new BigDecimal("2"), bigIntType);

        RelDataType rowType = new Builder(tf)
                .add("c1", intType)
                .add("c2", bigIntType)
                .build();

        Function<Object[], Object[]> project = expFactory.project(List.of(val1, val2), rowType);
        Object[] result = project.apply(new Object[]{null, null});

        assertArrayEquals(new Object[]{1, 2L}, result);
    }

    @Test
    public void testPredicate() {
        RexBuilder rexBuilder = Commons.rexBuilder();
        IgniteTypeFactory tf = Commons.typeFactory();

        RelDataType intType = tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType rowType = new Builder(tf)
                .add("c1", intType)
                .build();

        RexInputRef ref = rexBuilder.makeInputRef(rowType, 0);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, List.of(ref));

        Predicate<Object[]> predicate = expFactory.predicate(filter, rowType);
        assertFalse(predicate.test(new Object[]{1}));
    }

    @Test
    public void testBiPredicate() {
        RexBuilder rexBuilder = Commons.rexBuilder();
        IgniteTypeFactory tf = Commons.typeFactory();

        RelDataType intType = tf.createSqlType(SqlTypeName.INTEGER);
        RelDataType rowType = new Builder(tf)
                .add("c1", intType)
                .add("c2", intType)
                .build();

        RexInputRef ref1 = rexBuilder.makeInputRef(rowType, 0);
        RexInputRef ref2 = rexBuilder.makeInputRef(rowType, 1);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(ref1, ref2));

        BiPredicate<Object[], Object[]> predicate = expFactory.biPredicate(filter, rowType);
        assertFalse(predicate.test(new Object[]{0, 1}, new Object[]{1, 0}));
        assertTrue(predicate.test(new Object[]{0, 0}, new Object[]{0, 0}));
    }

    @Test
    public void testValues() {
        RexBuilder rexBuilder = Commons.rexBuilder();
        IgniteTypeFactory tf = Commons.typeFactory();

        RelDataType intType = tf.createSqlType(SqlTypeName.INTEGER);
        RelDataType bigIntType = tf.createSqlType(SqlTypeName.BIGINT);

        RexLiteral val10 = rexBuilder.makeExactLiteral(new BigDecimal("1"), intType);
        RexLiteral val11 = rexBuilder.makeExactLiteral(new BigDecimal("2"), bigIntType);
        RexLiteral val20 = rexBuilder.makeExactLiteral(new BigDecimal("3"), intType);
        RexLiteral val21 = rexBuilder.makeExactLiteral(new BigDecimal("4"), bigIntType);

        RelDataType rowType = new Builder(tf)
                .add("c1", intType)
                .add("c2", bigIntType)
                .build();

        List<List<Object>> actual = new ArrayList<>();
        expFactory.values(List.of(val10, val11, val20, val21), rowType).forEach(v -> actual.add(Arrays.asList(v)));

        assertEquals(List.of(List.of(1, 2L), List.of(3, 4L)), actual);
    }

    @Test
    public void testValuesEmpty() {
        IgniteTypeFactory tf = Commons.typeFactory();

        RelDataType intType = tf.createSqlType(SqlTypeName.INTEGER);
        RelDataType bigIntType = tf.createSqlType(SqlTypeName.BIGINT);

        RelDataType rowType = new Builder(tf)
                .add("c1", intType)
                .add("c2", bigIntType)
                .build();

        List<List<Object>> actual = new ArrayList<>();
        expFactory.values(List.of(), rowType).forEach(v -> actual.add(Arrays.asList(v)));

        assertEquals(List.of(), actual);
    }

    /**
     * Checks the execution of the {@link ExpressionFactory#rowSource(List)} method.
     * <ul>
     * <li>If the input list contains only constant expressions (literals), then row assembly must be performed without compiling the
     * expressions.</li>
     * <li>If the input list contains not only literals, then row assembly must be performed with compiling the expressions.</li>
     * </ul>
     *
     * @param columnType Column type.
     * @param literalsOnly Flag indicating that the list of input expressions should contain only literals.
     */
    @ParameterizedTest(name = "type={0}, literals={1}")
    @MethodSource("rowSourceTestArgs")
    public void testRowSource(ColumnType columnType, boolean literalsOnly) {
        Object val = SqlTestUtils.generateValueByTypeWithMaxScalePrecisionForSql(columnType);

        RexNode expr1 = SqlTestUtils.generateLiteralOrValueExpr(columnType, val);
        assertInstanceOf(RexLiteral.class, expr1);

        Object val2 = literalsOnly ? 1 : randomUUID();
        RexNode expr2 = SqlTestUtils.generateLiteralOrValueExpr(literalsOnly ? ColumnType.INT32 : ColumnType.UUID, val2);
        assertEquals(literalsOnly, expr2 instanceof RexLiteral);

        ExpressionFactoryImpl<Object[]> expFactorySpy = Mockito.spy(expFactory);

        Object[] actual = expFactorySpy.rowSource(List.of(expr1, expr2)).get();

        Object expected;

        if (columnType == ColumnType.FLOAT) {
            expected = ((BigDecimal) ((RexLiteral) expr1).getValue4()).floatValue();
        } else if (columnType == ColumnType.DOUBLE) {
            expected = ((BigDecimal) ((RexLiteral) expr1).getValue4()).doubleValue();
        } else {
            expected = val == null ? null : TypeUtils.toInternal(val, val.getClass());
        }

        assertEquals(Arrays.asList(expected, val2), Arrays.asList(actual));

        verify(expFactorySpy, times(literalsOnly ? 0 : 1)).scalar(any(), any());
    }

    @Test
    public void testExpression() {
        RexBuilder rexBuilder = Commons.rexBuilder();
        IgniteTypeFactory tf = Commons.typeFactory();

        RelDataType varcharType = tf.createSqlType(SqlTypeName.VARCHAR);
        Object actual = expFactory.execute(rexBuilder.makeLiteral("42", varcharType)).get();

        assertEquals("42", actual);
    }

    @ParameterizedTest
    @MethodSource("numericLiterals")
    public void testConvertNumericExact(RexLiteral lit, RelDataType dataType, @Nullable Object expected, boolean err) {
        RelDataType rowType = new Builder(typeFactory).add("c1", dataType).build();

        if (!err) {
            Object[] rowValues = expFactory.rowSource(List.of(lit)).get();
            assertArrayEquals(new Object[]{expected}, rowValues, "rowSource");

            Object[] litValues = expFactory.values(List.of(lit), rowType).iterator().next();
            assertArrayEquals(new Object[]{expected}, litValues, "values");
        } else {
            String errorMessage = "out of range";

            Supplier<Object[]> rowExpr = expFactory.rowSource(List.of(lit));
            assertThrowsSqlException(Sql.RUNTIME_ERR, errorMessage, rowExpr::get);

            assertThrowsSqlException(Sql.RUNTIME_ERR, errorMessage, () -> {
                expFactory.values(List.of(lit), rowType).iterator().next();
            });
        }
    }

    private static Stream<Arguments> numericLiterals() {
        RexBuilder rexBuilder = Commons.rexBuilder();
        // Make literal function
        BiFunction<Object, RelDataType, RexLiteral> makeLit = (v, t) -> (RexLiteral) rexBuilder.makeLiteral(v, t, false, false);

        RelDataType tinyInt = Commons.typeFactory().createSqlType(SqlTypeName.TINYINT);
        Object tinyIntMax = SqlTypeName.TINYINT.getLimit(true, Limit.OVERFLOW, true, -1, -1);
        Object tinyIntMin = SqlTypeName.TINYINT.getLimit(false, Limit.OVERFLOW, true, -1, -1);

        RelDataType shortType = Commons.typeFactory().createSqlType(SqlTypeName.SMALLINT);
        Object shortMax = SqlTypeName.SMALLINT.getLimit(true, Limit.OVERFLOW, true, -1, -1);
        Object shortMin = SqlTypeName.SMALLINT.getLimit(false, Limit.OVERFLOW, true, -1, -1);

        RelDataType intType = Commons.typeFactory().createSqlType(SqlTypeName.INTEGER);
        Object maxInt = SqlTypeName.INTEGER.getLimit(true, Limit.OVERFLOW, true, -1, -1);
        Object minInt = SqlTypeName.INTEGER.getLimit(false, Limit.OVERFLOW, true, -1, -1);

        RelDataType bigIntType = Commons.typeFactory().createSqlType(SqlTypeName.BIGINT);
        Object bigIntMax = SqlTypeName.BIGINT.getLimit(true, Limit.OVERFLOW, true, -1, -1);
        Object bigIntMin = SqlTypeName.BIGINT.getLimit(false, Limit.OVERFLOW, true, -1, -1);

        RelDataType realType = Commons.typeFactory().createSqlType(SqlTypeName.REAL);
        BigDecimal realMax = new BigDecimal(String.valueOf(Float.MAX_VALUE)).add(BigDecimal.ONE);
        Object realMin = realMax.negate();

        RelDataType floatType = Commons.typeFactory().createSqlType(SqlTypeName.FLOAT);
        BigDecimal floatMax = new BigDecimal(String.valueOf(Float.MAX_VALUE)).add(BigDecimal.ONE);
        Object floatMin = realMax.negate();

        RelDataType doubleType = Commons.typeFactory().createSqlType(SqlTypeName.DOUBLE);
        BigDecimal doubleMax = new BigDecimal(String.valueOf(Double.MAX_VALUE)).add(BigDecimal.ONE);
        Object doubleMin = doubleMax.negate();

        // makeLiteral for decimal validates precision/scale, so it is not possible to create a decimal literal
        // that lies outside of the range
        RelDataType decimal5 = Commons.typeFactory().createSqlType(SqlTypeName.DECIMAL, 5);
        RelDataType decimal52 = Commons.typeFactory().createSqlType(SqlTypeName.DECIMAL, 5, 2);

        return Stream.of(
                // TINYINT
                Arguments.of(makeLit.apply(BigDecimal.ONE, tinyInt), tinyInt, (byte) 1, false),
                Arguments.of(makeLit.apply(tinyIntMax, tinyInt), tinyInt, null, true),
                Arguments.of(makeLit.apply(tinyIntMin, tinyInt), tinyInt, null, true),

                // SMALLINT
                Arguments.of(makeLit.apply(BigDecimal.ONE, shortType), shortType, (short) 1, false),
                Arguments.of(makeLit.apply(shortMax, shortType), shortType, null, true),
                Arguments.of(makeLit.apply(shortMin, shortType), shortType, null, true),

                // INT
                Arguments.of(makeLit.apply(BigDecimal.ONE, intType), intType, 1, false),
                Arguments.of(makeLit.apply(maxInt, intType), intType, null, true),
                Arguments.of(makeLit.apply(minInt, intType), intType, null, true),

                // BIGINT
                Arguments.of(makeLit.apply(BigDecimal.ONE, bigIntType), bigIntType, 1L, false),
                Arguments.of(makeLit.apply(bigIntMax, bigIntType), bigIntType, null, true),
                Arguments.of(makeLit.apply(bigIntMin, bigIntType), bigIntType, null, true),

                // REAL
                Arguments.of(makeLit.apply(BigDecimal.ONE, realType), realType, 1.0f, false),
                Arguments.of(makeLit.apply(realMax, realType), realType, null, true),
                Arguments.of(makeLit.apply(realMin, realType), realType, null, true),

                // FLOAT
                Arguments.of(makeLit.apply(BigDecimal.ONE, floatType), floatType, 1.0f, false),
                Arguments.of(makeLit.apply(floatMax, floatType), floatType, null, true),
                Arguments.of(makeLit.apply(floatMin, floatType), floatType, null, true),

                // DOUBLE
                Arguments.of(makeLit.apply(BigDecimal.ONE, doubleType), doubleType, 1.0d, false),
                Arguments.of(makeLit.apply(doubleMax, doubleType), doubleType, null, true),
                Arguments.of(makeLit.apply(doubleMin, doubleType), doubleType, null, true),

                // DECIMAL
                Arguments.of(makeLit.apply(new BigDecimal("1"), decimal5), decimal5, new BigDecimal("1"), false),
                Arguments.of(makeLit.apply(new BigDecimal("1.0"), decimal52), decimal52, new BigDecimal("1.00"), false)
        );
    }

    private static List<Arguments> rowSourceTestArgs() {
        EnumSet<ColumnType> ignoredTypes = EnumSet.of(
                // UUID literal doesn't exists.
                ColumnType.UUID,
                // TODO https://issues.apache.org/jira/browse/IGNITE-17373
                ColumnType.DURATION,
                ColumnType.PERIOD
        );

        List<Arguments> arguments = new ArrayList<>();

        for (ColumnType columnType : ColumnType.values()) {
            if (ignoredTypes.contains(columnType)) {
                continue;
            }

            arguments.add(Arguments.of(columnType, true));
            arguments.add(Arguments.of(columnType, false));
        }

        return arguments;
    }

    static final class TestRange {

        final Object[] lower;

        final Object[] upper;

        final boolean lowerInclude;

        final boolean upperInclude;

        TestRange(Object[] lower) {
            this(lower, lower);
        }

        TestRange(Object @Nullable [] lower, Object @Nullable [] upper) {
            this(lower, true, upper, true);
        }

        TestRange(Object @Nullable [] lower, boolean lowerInclude, Object @Nullable [] upper, boolean upperInclude) {
            this.lower = lower;
            this.upper = upper;
            this.lowerInclude = lowerInclude;
            this.upperInclude = upperInclude;
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
            return lowerInclude == testRange.lowerInclude && upperInclude == testRange.upperInclude && Arrays.equals(lower,
                    testRange.lower) && Arrays.equals(upper, testRange.upper);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(lowerInclude, upperInclude);
            result = 31 * result + Arrays.hashCode(lower);
            result = 31 * result + Arrays.hashCode(upper);
            return result;
        }

        @Override
        public String toString() {
            return "{lower=" + Arrays.toString(lower)
                    + ", upper=" + Arrays.toString(upper)
                    + ", lowerInclude=" + lowerInclude
                    + ", upperInclude=" + upperInclude
                    + '}';
        }
    }
}
