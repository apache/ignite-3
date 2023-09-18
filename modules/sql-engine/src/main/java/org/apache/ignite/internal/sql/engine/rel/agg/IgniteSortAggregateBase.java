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

package org.apache.ignite.internal.sql.engine.rel.agg;

import static org.apache.ignite.internal.sql.engine.util.Commons.maxPrefix;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.trait.TraitsAwareIgniteRel;

/**
 * Defines common methods for {@link IgniteRel relational nodes} that implement sort-base aggregates.
 */
interface IgniteSortAggregateBase extends TraitsAwareIgniteRel {
    /**
     * Returns a bit set of the grouping fields.
     *
     * @return bit set of ordinals of grouping fields
     */
    ImmutableBitSet getGroupSet();

    /**
     * Performs propagation of {@link RelCollation collation trait}.
     *
     * <p>Sorted aggregate operate on already sorted input to perform aggregation/grouping.
     * Because of that such operator produces rows sorted by grouping columns, and it can produce
     * results that may satisfy required collation trait (ordering).
     *
     * <p>If a grouping set contains all required ordering columns, then inputs should provide
     * ordering that includes all required ordering columns + the rest of the columns used in the grouping set:

     * <pre>
     *       GROUP BY a, b, c ORDER BY a, b
     *       Input collation: (a, b, c)
     * </pre>
     *
     * <p>Otherwise this operator is unable to fully satisfy required ordering and we require collation trait based
     * columns from the grouping set, and the rest of the requirements is going to be satisfied by enforcer operator:
     * <pre>
     *     GROUP BY a, b ORDER BY a, b, c
     *     Input collation (a, b)
     *     Enforcer: adds ordering by c
     * </pre>
     *
     * @param nodeTraits Required relational node output traits.
     * @param inputTraits Traits of input nodes.
     *
     * @return Traits satisfied by this expression and traits that input nodes should satisfy.
     */
    @Override
    default Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(
            RelTraitSet nodeTraits, List<RelTraitSet> inputTraits
    ) {
        RelCollation required = TraitUtils.collation(nodeTraits);
        ImmutableBitSet requiredKeys = ImmutableBitSet.of(required.getKeys());
        RelCollation collation;

        if (getGroupSet().contains(requiredKeys)) {
            List<RelFieldCollation> newCollationFields = new ArrayList<>(getGroupSet().cardinality());
            newCollationFields.addAll(required.getFieldCollations());

            ImmutableBitSet keysLeft = getGroupSet().except(requiredKeys);

            keysLeft.forEach(fieldIdx -> newCollationFields.add(TraitUtils.createFieldCollation(fieldIdx)));

            collation = RelCollations.of(newCollationFields);
        } else {
            collation = TraitUtils.createCollation(getGroupSet().toList());
        }

        return Pair.of(nodeTraits.replace(collation),
                List.of(inputTraits.get(0).replace(collation)));
    }

    /** {@inheritDoc} */
    @Override
    default List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
            RelTraitSet nodeTraits, List<RelTraitSet> inputTraits
    ) {
        RelCollation inputCollation = TraitUtils.collation(inputTraits.get(0));

        IntList newCollationColls = maxPrefix(inputCollation.getKeys(), getGroupSet().asSet());

        if (newCollationColls.size() < getGroupSet().cardinality()) {
            return List.of();
        }

        List<RelFieldCollation> suitableCollations = inputCollation.getFieldCollations()
                .stream().filter(k -> newCollationColls.contains(k.getFieldIndex())).collect(Collectors.toList());

        return List.of(Pair.of(
                nodeTraits.replace(RelCollations.of(suitableCollations)),
                inputTraits
        ));
    }
}
