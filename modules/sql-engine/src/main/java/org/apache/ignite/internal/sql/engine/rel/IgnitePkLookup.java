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

package org.apache.ignite.internal.sql.engine.rel;

import static org.apache.ignite.internal.sql.engine.util.RexUtils.builder;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingService;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.jetbrains.annotations.Nullable;

/**
 * Relational operator that represents lookup by a primary key.
 *
 * <p>Note: at the moment, KV api requires an actual transaction which is object
 * of type {@link InternalTransaction} while distributed execution has access to
 * only certain {@link TxAttributes attributes} of a transaction. Given that node is
 * not supposed to be a part of distributed query plan, the following parts were
 * deliberately omitted:<ul>
 *     <li>this class doesn't implement {@link SourceAwareIgniteRel}, making it impossible
 *     to map properly by {@link MappingService}</li>
 *     <li>{@link IgniteRelVisitor} was not extended with this class</li>
 *     <li>de-serialisation constructor is omitted (see {@link ProjectableFilterableTableScan#ProjectableFilterableTableScan(RelInput)}
 *     as example)</li>
 * </ul>
 */
public class IgnitePkLookup extends ProjectableFilterableTableScan implements IgniteRel {
    private final List<RexNode> keyExpressions;

    private IgnitePkLookup(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelOptTable tbl,
            List<RelHint> hints,
            List<RexNode> keyExpressions,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        super(cluster, traits, hints, tbl, proj, cond, requiredColumns);

        this.keyExpressions = keyExpressions;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgnitePkLookup(cluster, getTraitSet(), getTable(), getHints(), keyExpressions, projects, condition, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public IgnitePkLookup withHints(List<RelHint> hintList) {
        return new IgnitePkLookup(getCluster(), getTraitSet(), getTable(), hintList, keyExpressions, projects, condition, requiredColumns);
    }

    @Override
    protected RelWriter explainTerms0(RelWriter pw) {
        return super.explainTerms0(pw)
                .item("key", keyExpressions);
    }

    /**
     * Returns a list of expressions in the order of a primary key columns of related table
     * to use as lookup key.
     *
     * @return List of expressions representing lookup key.
     */
    public List<RexNode> keyExpressions() {
        return keyExpressions;
    }

    /**
     * Tries to convert given scan node to physical node representing primary key lookup.
     *
     * <p>Conversion will be successful if: <ol>
     *     <li>there is condition</li>
     *     <li>table has primary key index</li>
     *     <li>condition covers all columns of primary key index</li>
     *     <li>only single search key is derived from condition</li>
     * </ol>
     *
     * @param scan A scan node to analyze.
     * @return A physical node representing optimized look up by primary key or {@code null}
     *      if we were unable to derive all necessary information.
     */
    public static @Nullable IgnitePkLookup convert(IgniteLogicalTableScan scan) {
        List<SearchBounds> bounds = deriveSearchBounds(scan);

        if (nullOrEmpty(bounds)) {
            return null;
        }

        List<RexNode> expressions = new ArrayList<>(bounds.size());

        RelOptCluster cluster = scan.getCluster();
        RexBuilder rexBuilder = builder(cluster);
        RexNode originalCondition = RexUtil.toCnf(rexBuilder, scan.condition());
        Set<RexNode> condition = new HashSet<>(RelOptUtil.conjunctions(originalCondition));

        for (SearchBounds bound : bounds) {
            // iteration over a number of search keys are not supported yet,
            // thus we need to make sure only single key was derived
            if (!(bound instanceof ExactBounds)) {
                return null;
            }

            condition.remove(bound.condition());
            expressions.add(((ExactBounds) bound).bound());
        }

        if (nullOrEmpty(expressions)) {
            return null;
        }

        RexNode resultingCondition = RexUtil.composeConjunction(rexBuilder, condition);
        if (resultingCondition.isAlwaysTrue()) {
            resultingCondition = null;
        }

        return new IgnitePkLookup(
                cluster,
                scan.getTraitSet()
                        .replace(IgniteConvention.INSTANCE)
                        .replace(IgniteDistributions.single()),
                scan.getTable(),
                scan.getHints(),
                expressions,
                scan.projects(),
                resultingCondition,
                scan.requiredColumns()
        );
    }

    private static @Nullable List<SearchBounds> deriveSearchBounds(IgniteLogicalTableScan scan) {
        RexNode condition = scan.condition();

        if (condition == null) {
            return null;
        }

        IgniteTable table = scan.getTable().unwrap(IgniteTable.class);

        assert table != null : scan.getTable();

        IgniteIndex primaryKeyIndex = table.indexes().values().stream()
                .filter(IgniteIndex::primaryKey)
                .findAny()
                .orElse(null);

        if (primaryKeyIndex == null) {
            return null;
        }

        RelOptCluster cluster = scan.getCluster();
        RelCollation collation = primaryKeyIndex.collation();
        ImmutableBitSet requiredColumns = scan.requiredColumns();
        RelDataType rowType = table.getRowType(cluster.getTypeFactory());

        if (requiredColumns != null) {
            Mappings.TargetMapping targetMapping = Commons.trimmingMapping(
                    rowType.getFieldCount(), requiredColumns);

            collation = collation.apply(targetMapping);
        }

        return RexUtils.buildHashSearchBounds(
                cluster,
                collation,
                condition,
                rowType,
                requiredColumns
        );
    }
}
