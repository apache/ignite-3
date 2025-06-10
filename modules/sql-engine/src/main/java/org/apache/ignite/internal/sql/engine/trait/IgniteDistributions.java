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

package org.apache.ignite.internal.sql.engine.trait;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.util.ImmutableIntList;

/**
 * IgniteDistributions.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class IgniteDistributions {
    private static final IgniteDistribution BROADCAST = canonize(new DistributionTrait(DistributionFunction.broadcast()));

    private static final IgniteDistribution SINGLETON = canonize(new DistributionTrait(DistributionFunction.singleton()));

    private static final IgniteDistribution RANDOM = canonize(new DistributionTrait(DistributionFunction.random()));

    private static final IgniteDistribution ANY = canonize(new DistributionTrait(DistributionFunction.any()));

    /**
     * Get any distribution.
     */
    public static IgniteDistribution any() {
        return ANY;
    }

    /**
     * Get random distribution.
     */
    public static IgniteDistribution random() {
        return RANDOM;
    }

    /**
     * Get single distribution.
     */
    public static IgniteDistribution single() {
        return SINGLETON;
    }

    /**
     * Get broadcast distribution.
     */
    public static IgniteDistribution broadcast() {
        return BROADCAST;
    }

    /**
     * Creates an affinity distribution that takes into account the zone ID and calculates the destinations
     * based on a hash function which takes into account the key field types of the row.
     *
     * @param keys Affinity keys ordinals. Should not be null or empty.
     * @param tableId Table ID.
     * @param zoneId  Distribution zone ID.
     * @param label Human-readable label to show in EXPLAIN printout.
     * @return Affinity distribution.
     */
    public static IgniteDistribution affinity(List<Integer> keys, int tableId, int zoneId, String label) {
        assert !nullOrEmpty(keys) : "Hash-based distribution must have at least one key";

        return canonize(new DistributionTrait(keys, tableId, zoneId, label, DistributionFunction.hash()));
    }

    /**
     * Creates a hash distribution that calculates destinations based on a composite hash of key field values of the row.
     *
     * @param keys Distribution keys ordinals. Should not be null or empty.
     * @return Hash distribution.
     */
    public static IgniteDistribution hash(List<Integer> keys) {
        return hash(keys, DistributionFunction.hash());
    }

    /**
     * Creates a hash distribution that calculates destinations based on a composite hash of key field values of the row.
     *
     * @param keys Distribution keys ordinals. Should not be null or empty.
     * @param function Specific hash function.
     * @return Hash distribution.
     */
    public static IgniteDistribution hash(List<Integer> keys, DistributionFunction function) {
        assert !nullOrEmpty(keys) : "Hash-based distribution must have at least one key";

        return canonize(new DistributionTrait(keys, function));
    }

    /**
     * Creates an identity distribution that calculates destinations based on a raw value of the row field.
     *
     * @param key Distribution key ordinal.
     * @return Identity distribution.
     */
    public static IgniteDistribution identity(int key) {
        return canonize(new DistributionTrait(ImmutableIntList.of(key), DistributionFunction.identity()));
    }

    /**
     * Creates a distribution trait of the same hash function with given distribution keys.
     *
     * @param trait Distribution trait.
     * @param keys Distribution keys ordinals. Should not be null or empty.
     * @return Distribution trait.
     */
    public static IgniteDistribution clone(IgniteDistribution trait, List<Integer> keys) {
        assert !nullOrEmpty(keys) : "Hash-based distribution must have at least one key";
        assert trait.function().type() == Type.HASH_DISTRIBUTED;

        DistributionTrait distributionTrait = trait.isTableDistribution()
                ? new DistributionTrait(keys, trait.tableId(), trait.zoneId(), trait.label(), trait.function())
                : new DistributionTrait(keys, trait.function());

        return canonize(distributionTrait);
    }

    /**
     * See {@link RelTraitDef#canonize(org.apache.calcite.plan.RelTrait)}.
     */
    private static IgniteDistribution canonize(IgniteDistribution distr) {
        return DistributionTraitDef.INSTANCE.canonize(distr);
    }
}
