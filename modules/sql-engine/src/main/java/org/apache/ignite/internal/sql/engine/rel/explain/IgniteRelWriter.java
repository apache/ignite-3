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

package org.apache.ignite.internal.sql.engine.rel.explain;

import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;

/**
 * A fluent interface for explaining the structure and metadata of an {@link IgniteRel}
 * (relational operator) node.
 */
@SuppressWarnings("UnusedReturnValue")
public interface IgniteRelWriter {

    /**
     * Adds a list of projection expressions to the current relational node.
     *
     * @param projections List of {@link RexNode} expressions representing the projected output.
     * @param rowType The row type against which field indexes in the projections should be resolved.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addProjection(List<RexNode> projections, RelDataType rowType);

    /**
     * Adds a filtering predicate applied to the input rows of the relational node.
     *
     * @param condition A {@link RexNode} representing the filtering condition.
     * @param rowType The row type against which field indexes in the predicate should be resolved.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addPredicate(RexNode condition, RelDataType rowType);

    /**
     * Adds table metadata to the current relational node.
     *
     * @param table A {@link RelOptTable} representing the referenced table.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addTable(RelOptTable table);

    /**
     * Adds index metadata, including name and type, associated with the relational node.
     *
     * @param name The name of the index.
     * @param type The type of the index, from {@link IgniteIndex.Type}.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addIndex(String name, IgniteIndex.Type type);

    /**
     * Adds collation (sort order) information for the node’s output rows.
     *
     * @param collation The {@link RelCollation} defining sort directions.
     * @param rowType The row type against which field indexes in the collation should be resolved.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addCollation(RelCollation collation, RelDataType rowType);

    /**
     * Adds literal tuples that represent either static data or example results.
     *
     * @param tuples A list of row tuples, each consisting of {@link RexLiteral} values.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addTuples(List<List<RexLiteral>> tuples);

    /**
     * Adds the source fragment ID, indicating the origin of a data flow edge between fragments.
     *
     * @param fragmentId The ID of the source fragment.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addSourceFragmentId(long fragmentId);

    /**
     * Adds the target fragment ID, indicating the destination of a data flow edge between fragments.
     *
     * @param fragmentId The ID of the target fragment.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addTargetFragmentId(long fragmentId);

    /**
     * Adds a child relational node (i.e., an input) to the current node.
     *
     * @param rel The child {@link IgniteRel} node.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addChild(IgniteRel rel);

    /**
     * Adds join type metadata for join operations.
     *
     * @param joinType The {@link JoinRelType} (e.g., INNER, LEFT, SEMI).
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addJoinType(JoinRelType joinType);

    /**
     * Adds distribution information that describes how the rows are distributed across nodes.
     *
     * @param distribution The {@link IgniteDistribution} strategy.
     * @param rowType The row type against which field indexes in the distribution should be resolved.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addDistribution(IgniteDistribution distribution, RelDataType rowType);

    /**
     * Adds source expressions used for row computations. Mainly used by {@link IgniteKeyValueModify}.
     *
     * @param expressions List of {@link RexNode} expressions representing source values.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addSourceExpressions(List<RexNode> expressions);

    /**
     * Adds the type of data modification operation (e.g., INSERT, UPDATE, DELETE).
     *
     * @param operation The type of modification operation.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addModifyOperationType(Operation operation);

    /**
     * Adds grouping key information used in aggregation operations.
     *
     * @param groupSet The {@link ImmutableBitSet} representing grouping key indexes.
     * @param rowType The row type against which field indexes in the group set should be resolved.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addGroup(ImmutableBitSet groupSet, RelDataType rowType);

    /**
     * Adds multiple group sets used for advanced grouping (e.g., ROLLUP or CUBE).
     *
     * @param groupSets List of {@link ImmutableBitSet} instances.
     * @param rowType The row type against which field indexes in the group set should be resolved.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addGroupSets(List<ImmutableBitSet> groupSets, RelDataType rowType);

    /**
     * Adds aggregation function calls applied to grouped data.
     *
     * @param aggCalls List of {@link AggregateCall} objects defining aggregations.
     * @param rowType The row type against which field indexes in the aggregate should be resolved.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addAggregation(List<AggregateCall> aggCalls, RelDataType rowType);

    /**
     * Adds expressions forming a key for {@link IgniteKeyValueGet lookup-by-a-key} node.
     *
     * @param expressions List of {@link RexNode} expressions representing key.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addKeyExpression(List<RexNode> expressions);

    /**
     * Adds correlated variables which are set by {@link IgniteCorrelatedNestedLoopJoin}.
     *
     * @param variablesSet Set of {@link CorrelationId} identifiers.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addCorrelatedVariables(Set<CorrelationId> variablesSet);

    /**
     * Adds search bounds used for index look ups.
     *
     * @param searchBounds List of {@link SearchBounds} representing boundaries of the range scan or point look up.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addSearchBounds(List<SearchBounds> searchBounds);

    /**
     * Adds an invocation of an expressions. Mainly used by {@link IgniteTableFunctionScan} rel.
     *
     * @param call A {@link RexNode} representing the invocation.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addInvocation(RexNode call);

    /**
     * Adds an expressions denoting how many rows should be skipped before emitting a result.
     *
     * @param offset A {@link RexNode} representing the number of rows to skip.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addOffset(RexNode offset);

    /**
     * Adds an expressions denoting how many rows should be returned.
     *
     * @param fetch A {@link RexNode} representing the number of rows to return.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addFetch(RexNode fetch);

    /**
     * Adds a notion whether set operation does rows deduplication or not.
     *
     * @param all A flag denoting whether rows will be de-duplicated.
     * @return This writer instance for chaining.
     */
    IgniteRelWriter addAll(boolean all);
}
