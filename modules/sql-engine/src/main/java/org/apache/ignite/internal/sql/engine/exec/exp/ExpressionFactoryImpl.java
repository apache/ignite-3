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

import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;

import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorsFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Implements rex expression into a function object. Uses JaninoRexCompiler under the hood. Each expression compiles into a class and a
 * wrapper over it is returned.
 */
public class ExpressionFactoryImpl<RowT> implements ExpressionFactory<RowT> {
    private static final IgniteTypeFactory TYPE_FACTORY = Commons.typeFactory();
    private static final RexBuilder REX_BUILDER = Commons.rexBuilder();
    private static final SqlConformance SQL_CONFORMANCE = FRAMEWORK_CONFIG.getParserConfig().conformance();

    private final IgniteTypeFactory typeFactory;
    private final ComparatorImplementor comparatorImplementor;
    private final JoinPredicateImplementor joinPredicateImplementor;
    private final PredicateImplementor predicateImplementor;
    private final JoinProjectionImplementor joinProjectionImplementor;
    private final ProjectionImplementor projectionImplementor;
    private final RowProviderImplementor rowProviderImplementor;
    private final ScalarImplementor scalarImplementor;
    private final SearchBoundsImplementor searchBoundsImplementor;
    private final ValuesImplementor valuesImplementor;

    /**
     * Constructs the object.
     *
     * @param typeFactory The type factory to convert between relational and java types.
     * @param cacheSize The size of the cache to store compiled expressions.
     * @param cacheFactory The factory to create cache to store compiled expressions in.
     */
    public ExpressionFactoryImpl(
            IgniteTypeFactory typeFactory,
            int cacheSize,
            CacheFactory cacheFactory
    ) {
        this.typeFactory = typeFactory;

        Cache<String, Object> cache = cacheFactory.create(cacheSize);

        comparatorImplementor = new ComparatorImplementor();
        joinPredicateImplementor = new JoinPredicateImplementor(
                cache, REX_BUILDER, TYPE_FACTORY, SQL_CONFORMANCE
        );
        predicateImplementor = new PredicateImplementor(
                cache, REX_BUILDER, TYPE_FACTORY, SQL_CONFORMANCE
        );
        joinProjectionImplementor = new JoinProjectionImplementor(
                cache, REX_BUILDER, TYPE_FACTORY, SQL_CONFORMANCE
        );
        projectionImplementor = new ProjectionImplementor(
                cache, REX_BUILDER, TYPE_FACTORY, SQL_CONFORMANCE
        );
        rowProviderImplementor = new RowProviderImplementor(
                cache, REX_BUILDER, TYPE_FACTORY, SQL_CONFORMANCE
        );
        scalarImplementor = new ScalarImplementor(
                cache, REX_BUILDER, TYPE_FACTORY, SQL_CONFORMANCE
        );
        searchBoundsImplementor = new SearchBoundsImplementor();
        valuesImplementor = new ValuesImplementor(TYPE_FACTORY);
    }

    /** {@inheritDoc} */
    @Override
    public AccumulatorsFactory<RowT> accumulatorsFactory(
            AggregateType type,
            List<AggregateCall> calls,
            RelDataType rowType
    ) {
        assert !calls.isEmpty();

        return new AccumulatorsFactory<>(type, typeFactory, calls, rowType);
    }

    /** {@inheritDoc} */
    @Override
    public SqlComparator<RowT> comparator(RelCollation collation) {
        return comparatorImplementor.implement(collation);
    }

    /** {@inheritDoc} */
    @Override
    public SqlComparator<RowT> comparator(List<RelFieldCollation> left, List<RelFieldCollation> right, ImmutableBitSet equalNulls) {
        return comparatorImplementor.implement(left, right, equalNulls);
    }

    /** {@inheritDoc} */
    @Override
    public SqlPredicate<RowT> predicate(RexNode filter, RelDataType rowType) {
        return predicateImplementor.implement(filter, rowType);
    }

    /** {@inheritDoc} */
    @Override
    public SqlJoinPredicate<RowT> joinPredicate(RexNode filter, RelDataType rowType, int firstRowSize) {
        return joinPredicateImplementor.implement(filter, rowType, firstRowSize);
    }

    /** {@inheritDoc} */
    @Override
    public SqlProjection<RowT> project(List<RexNode> projects, RelDataType inputRowType) {
        return projectionImplementor.implement(projects, inputRowType);
    }

    /** {@inheritDoc} */
    @Override
    public SqlJoinProjection<RowT> joinProject(List<RexNode> projects, RelDataType rowType, int firstRowSize) {
        return joinProjectionImplementor.implement(projects, rowType, firstRowSize);
    }

    /** {@inheritDoc} */
    @Override
    public SqlRowProvider<RowT> rowSource(List<RexNode> values) {
        return rowProviderImplementor.implement(values);
    }

    /** {@inheritDoc} */
    @Override
    public <T> SqlScalar<RowT, T> scalar(RexNode node) {
        return scalarImplementor.implement(node);
    }

    /** {@inheritDoc} */
    @Override
    public SqlScalar<RowT, List<RowT>> values(List<List<RexLiteral>> values, RelDataType rowType) {
        return valuesImplementor.implement(values, rowType);
    }

    /** {@inheritDoc} */
    @Override
    public SqlScalar<RowT, RangeIterable<RowT>> ranges(
            List<SearchBounds> searchBounds,
            RelDataType rowType,
            @Nullable SqlComparator<RowT> comparator
    ) {
        return searchBoundsImplementor.implement(searchBounds, rowType, comparator, this::rowSource, this::scalar);
    }

    static String digest(Class<?> clazz, List<? extends RexNode> nodes, @Nullable RelDataType type) {
        return digest(clazz, nodes, type, null);
    }

    static String digest(
            Class<?> clazz, List<? extends RexNode> nodes, @Nullable RelDataType type, @Nullable Object additionalContext
    ) {
        StringBuilder b = new StringBuilder(clazz.getSimpleName());

        b.append('[');

        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                b.append(';');
            }

            RexNode node = nodes.get(i);

            b.append(node);

            if (node == null) {
                continue;
            }

            b.append(':').append(node.getType().getFullTypeString());

            new RexShuttle() {
                @Override
                public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                    b.append(", fldIdx=").append(fieldAccess.getField().getIndex());

                    return super.visitFieldAccess(fieldAccess);
                }

                @Override public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
                    b.append(", paramType=").append(dynamicParam.getType().getFullTypeString());

                    return super.visitDynamicParam(dynamicParam);
                }
            }.apply(node);
        }

        b.append(']');

        if (type != null) {
            b.append(':').append(type.getFullTypeString());
        }

        if (additionalContext != null) {
            b.append(',').append(additionalContext);
        }

        return b.toString();
    }
}
