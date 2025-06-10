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

import static org.apache.calcite.rel.RelDistribution.Type.ANY;
import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.RANDOM_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

/**
 * Description of the physical distribution of a relational expression.
 */
public final class DistributionTrait implements IgniteDistribution {
    private static final Comparator<Iterable<Integer>> ORDERING = (iterable0, iterable1) -> {
        Iterator<Integer> it0 = iterable0.iterator();
        Iterator<Integer> it1 = iterable1.iterator();

        while (it0.hasNext()) {
            if (!it1.hasNext()) {
                return 1;
            }

            int result = Integer.compare(it0.next(), it1.next());

            if (result != 0) {
                return result;
            }
        }

        if (it1.hasNext()) {
            return -1;
        }

        return 0;
    };

    private final DistributionFunction function;

    private final ImmutableIntList keys;

    private final boolean affinityFlag;

    private final int tableId;

    private final int zoneId;

    private final String label;

    /**
     * Constructor non-hash distributions.
     *
     * @param function Distribution function.
     */
    DistributionTrait(DistributionFunction function) {
        assert function.type() != HASH_DISTRIBUTED;

        this.function = function;

        keys = ImmutableIntList.of();
        affinityFlag = false;
        tableId = -1;
        zoneId = -1;
        label = function.name();
    }

    /**
     * Constructor for hash distribution.
     *
     * @param keys     Distribution keys.
     * @param function Distribution function.
     */
    DistributionTrait(List<Integer> keys, DistributionFunction function) {
        this(keys, -1, -1, function.name(), function, false);
    }

    /**
     * Constructor for affinity distribution.
     *
     * @param keys     Distribution keys.
     * @param function Distribution function.
     */
    DistributionTrait(List<Integer> keys, int tableId, int zoneId, String label, DistributionFunction function) {
        this(keys, tableId, zoneId, label, function, true);
    }

    private DistributionTrait(
            List<Integer> keys,
            int tableId,
            int zoneId,
            String label,
            DistributionFunction function,
            boolean affinityFlag
    ) {
        assert function.type() == HASH_DISTRIBUTED;

        this.keys = ImmutableIntList.copyOf(keys);
        this.function = function;
        this.tableId = tableId;
        this.zoneId = zoneId;
        this.label = label;
        this.affinityFlag = affinityFlag;
    }

    /** {@inheritDoc} */
    @Override
    public Type getType() {
        return function.type();
    }

    /** {@inheritDoc} */
    @Override
    public DistributionFunction function() {
        return function;
    }

    /** {@inheritDoc} */
    @Override
    public ImmutableIntList getKeys() {
        return keys;
    }

    @Override
    public boolean isTableDistribution() {
        return affinityFlag;
    }

    @Override
    public int tableId() {
        return tableId;
    }

    @Override
    public int zoneId() {
        return zoneId;
    }

    @Override
    public String label() {
        return label;
    }

    /** {@inheritDoc} */
    @Override
    public void register(RelOptPlanner planner) {
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof DistributionTrait) {
            DistributionTrait that = (DistributionTrait) o;

            return Objects.equals(function, that.function)
                    && Objects.equals(keys, that.keys)
                    && affinityFlag == that.affinityFlag
                    && tableId == that.tableId
                    && zoneId == that.zoneId
                    && Objects.equals(label, that.label);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(function, keys, zoneId, tableId);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return function.name()
                + (function.type() == HASH_DISTRIBUTED ? keys : "")
                + (isTableDistribution() ? "[zoneId=" + zoneId + ", tableId=" + tableId + ']' : "");
    }

    /** {@inheritDoc} */
    @Override
    public DistributionTraitDef getTraitDef() {
        return DistributionTraitDef.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override
    public boolean satisfies(RelTrait trait) {
        if (trait == this) {
            return true;
        }

        if (!(trait instanceof DistributionTrait)) {
            return false;
        }

        DistributionTrait other = (DistributionTrait) trait;

        if (other.getType() == ANY) {
            return true;
        }

        if (getType() == other.getType()) {
            return getType() != HASH_DISTRIBUTED
                    || (Objects.equals(keys, other.keys)
                    && affinityFlag == other.affinityFlag
                    && zoneId == other.zoneId
                    && Objects.equals(function, other.function));
        }

        if (other.getType() == RANDOM_DISTRIBUTED) {
            return getType() == HASH_DISTRIBUTED;
        }

        return other.getType() == SINGLETON && getType() == BROADCAST_DISTRIBUTED;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution apply(Mappings.TargetMapping mapping) {
        if (getType() != HASH_DISTRIBUTED) {
            return this;
        }

        for (int key : keys) {
            if (mapping.getTargetOpt(key) == -1) {
                return IgniteDistributions.random(); // Some distribution keys are not mapped => any.
            }
        }

        List<Integer> res = Mappings.apply2((Mapping) mapping, keys);

        return affinityFlag
                ? IgniteDistributions.affinity(res, tableId, zoneId, label)
                : IgniteDistributions.hash(res, function);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isTop() {
        return getType() == ANY;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(RelMultipleTrait o) {
        final IgniteDistribution distribution = (IgniteDistribution) o;

        if (getType() == distribution.getType() && getType() == HASH_DISTRIBUTED) {
            int cmp = ORDERING.compare(getKeys(), distribution.getKeys());

            if (cmp == 0) {
                cmp = function.name().compareTo(distribution.function().name());
            }

            if (cmp == 0) {
                cmp = Boolean.compare(affinityFlag, distribution.isTableDistribution());
            }

            if (cmp == 0 && affinityFlag) {
                cmp = Integer.compare(zoneId, distribution.zoneId());

                if (cmp == 0) {
                    cmp = Integer.compare(tableId, distribution.tableId());
                }

                if (cmp == 0) {
                    cmp = label.compareTo(distribution.label());
                }
            }

            return cmp;
        }

        return getType().compareTo(distribution.getType());
    }
}
