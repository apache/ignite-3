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
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.trait.TraitsAwareIgniteRel;

/**
 * IgniteSortAggregateBase interface.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
interface IgniteSortAggregateBase extends TraitsAwareIgniteRel {
    /**
     * Returns a bit set of the grouping fields.
     *
     * @return bit set of ordinals of grouping fields
     */
    ImmutableBitSet getGroupSet();

    /** {@inheritDoc} */
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
