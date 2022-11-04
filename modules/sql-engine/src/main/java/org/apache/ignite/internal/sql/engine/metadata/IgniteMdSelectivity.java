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

package org.apache.ignite.internal.sql.engine.metadata;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.AbstractIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.util.RexUtils;

/**
 * IgniteMdSelectivity.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class IgniteMdSelectivity extends RelMdSelectivity {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.SELECTIVITY.method, new IgniteMdSelectivity());

    /**
     * GetSelectivity.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public Double getSelectivity(AbstractIndexScan rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null) {
            return getSelectivity((ProjectableFilterableTableScan) rel, mq, predicate);
        }

        List<SearchBounds> searchBounds = rel.searchBounds();

        if (nullOrEmpty(searchBounds)) {
            return RelMdUtil.guessSelectivity(rel.condition());
        }

        double idxSelectivity = 1.0;

        List<RexNode> conjunctions = RelOptUtil.conjunctions(rel.condition());

        for (SearchBounds bounds : searchBounds) {
            if (bounds != null) {
                conjunctions.remove(bounds.condition());
            }

            idxSelectivity *= guessCostMultiplier(bounds);
        }

        RexNode remaining = RexUtil.composeConjunction(RexUtils.builder(rel), conjunctions, true);

        return idxSelectivity * RelMdUtil.guessSelectivity(remaining);
    }

    /**
     * GetSelectivity.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public Double getSelectivity(ProjectableFilterableTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate == null) {
            return RelMdUtil.guessSelectivity(rel.condition());
        }

        RexNode condition = rel.pushUpPredicate();
        if (condition == null) {
            return RelMdUtil.guessSelectivity(predicate);
        }

        RexNode diff = RelMdUtil.minusPreds(RexUtils.builder(rel), predicate, condition);
        return RelMdUtil.guessSelectivity(diff);
    }

    /**
     * GetSelectivity.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public Double getSelectivity(IgniteSortedIndexSpool rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null) {
            return mq.getSelectivity(rel.getInput(),
                    RelMdUtil.minusPreds(
                            rel.getCluster().getRexBuilder(),
                            predicate,
                            rel.condition()));
        }

        return mq.getSelectivity(rel.getInput(), rel.condition());
    }

    /**
     * GetSelectivity.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public Double getSelectivity(IgniteHashIndexSpool rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null) {
            return mq.getSelectivity(rel.getInput(),
                    RelMdUtil.minusPreds(
                            rel.getCluster().getRexBuilder(),
                            predicate,
                            rel.condition()));
        }

        return mq.getSelectivity(rel.getInput(), rel.condition());
    }

    /** Guess cost multiplier regarding search bounds only. */
    private static double guessCostMultiplier(SearchBounds bounds) {
        if (bounds instanceof ExactBounds) {
            return .1;
        } else if (bounds instanceof RangeBounds) {
            RangeBounds rangeBounds = (RangeBounds) bounds;

            if (rangeBounds.condition() != null) {
                return ((RexCall) rangeBounds.condition()).op.kind == SqlKind.EQUALS ? .1 : .2;
            } else {
                return .35;
            }
        } else if (bounds instanceof MultiBounds) {
            MultiBounds multiBounds = (MultiBounds) bounds;

            return multiBounds.bounds().stream()
                    .mapToDouble(IgniteMdSelectivity::guessCostMultiplier)
                    .max()
                    .orElseThrow(AssertionError::new);
        }

        return 1.0;
    }
}
