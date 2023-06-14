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

package org.apache.ignite.internal.sql.engine.rel.set;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Physical node for MAP phase of set op (MINUS, INTERSECT).
 */
public interface IgniteMapSetOp extends IgniteSetOp {
    /** {@inheritDoc} */
    @Override
    default List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
            RelTraitSet nodeTraits,
            List<RelTraitSet> inputTraits
    ) {
        if (inputTraits.stream().allMatch(t -> TraitUtils.distribution(t).satisfies(IgniteDistributions.single()))) {
            return List.of(); // If all distributions are single or broadcast IgniteSingleMinus should be used.
        }

        return List.of(
                Pair.of(nodeTraits.replace(IgniteDistributions.random()), Commons.transform(inputTraits,
                        t -> TraitUtils.distribution(t) == IgniteDistributions.broadcast()
                                // Allow broadcast with trim-exchange to be used in map-reduce set-op.
                                ? t.replace(IgniteDistributions.hash(List.of(0))) :
                                t.replace(IgniteDistributions.random())))
        );
    }

    /** Build RowType for MAP node. */
    default RelDataType buildRowType() {
        RelDataTypeFactory typeFactory = Commons.typeFactory(getCluster());

        assert typeFactory instanceof IgniteTypeFactory;

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        builder.add("GROUP_KEY", typeFactory.createJavaType(GroupKey.class));
        builder.add("COUNTERS", typeFactory.createJavaType(int[].class));

        return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    default AggregateType aggregateType() {
        return AggregateType.MAP;
    }
}
