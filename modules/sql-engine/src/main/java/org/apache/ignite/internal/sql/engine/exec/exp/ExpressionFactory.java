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

import java.util.Comparator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.jetbrains.annotations.Nullable;

/**
 * Expression factory.
 */
public interface ExpressionFactory<RowT> {
    Supplier<List<AccumulatorWrapper<RowT>>> accumulatorsFactory(
            AggregateType type,
            List<AggregateCall> calls,
            RelDataType rowType
    );

    /**
     * Creates a comparator for given data type and collations. Mainly used for sorted exchange.
     *
     * @param collations Collations.
     * @return Row comparator.
     */
    Comparator<RowT> comparator(RelCollation collations);

    /**
     * Creates a comparator for different rows by given field collations. Mainly used for merge join rows comparison. Note: Both list has to
     * have the same size and matched fields collations has to have the same traits (i.e. all pairs of field collations should have the same
     * sorting and nulls ordering).
     *
     * @param left  Collations of left row.
     * @param right Collations of right row.
     * @param equalNulls Bitset with null comparison strategy, use in case of NOT DISTINCT FROM syntax.
     * @return Rows comparator.
     */
    Comparator<RowT> comparator(List<RelFieldCollation> left, List<RelFieldCollation> right, ImmutableBitSet equalNulls);

    /**
     * Creates a Filter predicate.
     *
     * @param filter  Filter expression.
     * @param rowType Input row type.
     * @return Filter predicate.
     */
    Predicate<RowT> predicate(RexNode filter, RelDataType rowType);

    /**
     * Creates a Filter predicate.
     *
     * @param filter Filter expression.
     * @param rowType Input row type.
     * @return Filter predicate.
     */
    BiPredicate<RowT, RowT> biPredicate(RexNode filter, RelDataType rowType);

    /**
     * Creates a Project function. Resulting function returns a row with different fields, fields order, fields types, etc.
     *
     * @param projects Projection expressions.
     * @param rowType  Input row type.
     * @return Project function.
     */
    Function<RowT, RowT> project(List<RexNode> projects, RelDataType rowType);

    /**
     * Creates a Values relational node rows source.
     *
     * @param values  Values.
     * @param rowType Output row type.
     * @return Values relational node rows source.
     */
    Iterable<RowT> values(List<RexLiteral> values, RelDataType rowType);

    /**
     * Creates row from RexNodes.
     *
     * @param values Values ({@code null} values allowed in this list).
     * @return Row.
     */
    Supplier<RowT> rowSource(List<RexNode> values);

    /**
     * Creates iterable search bounds tuples (lower row/upper row) by search bounds expressions.
     *
     * @param searchBounds Search bounds.
     * @param rowType Row type.
     * @param comparator Comparator to return bounds in particular order.
     */
    RangeIterable<RowT> ranges(
            List<SearchBounds> searchBounds,
            RelDataType rowType,
            @Nullable Comparator<RowT> comparator
    );

    /**
     * Executes expression.
     */
    <T> Supplier<T> execute(RexNode node);
}
