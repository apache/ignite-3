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

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.metadata.CyclicMetadataException;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * IgniteMdDistinctRowCount.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdDistinctRowCount extends RelMdDistinctRowCount {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.DISTINCT_ROW_COUNT.method, new IgniteMdDistinctRowCount());

    @Override
    public Double getDistinctRowCount(Aggregate rel, RelMetadataQuery mq, ImmutableBitSet groupKey, @Nullable RexNode predicate) {
        return rel.estimateRowCount(mq);
    }

    /** {@inheritDoc} */
    @Override
    public Double getDistinctRowCount(
            RelSubset rel,
            RelMetadataQuery mq,
            ImmutableBitSet groupKey,
            RexNode predicate
    ) {
        RelNode best = rel.getBest();
        if (best != null) {
            return mq.getDistinctRowCount(best, groupKey, predicate);
        }

        Double d = null;
        for (RelNode r2 : rel.getRels()) {
            try {
                Double d2 = mq.getDistinctRowCount(r2, groupKey, predicate);
                d = NumberUtil.min(d, d2);
            } catch (CyclicMetadataException e) {
                // Ignore this relational expression; there will be non-cyclic ones
                // in this set.
            }
        }

        return d;
    }
}
