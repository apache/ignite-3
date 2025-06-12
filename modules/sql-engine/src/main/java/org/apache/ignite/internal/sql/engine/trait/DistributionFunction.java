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

import java.util.Objects;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelNode;

/**
 * Distribution function.
 */
public abstract class DistributionFunction {
    private String name;

    private DistributionFunction() {
        // No-op.
    }

    /**
     * Get distribution function type.
     */
    public abstract RelDistribution.Type type();

    /**
     * Get function name. This name used for equality checking and in {@link RelNode#getDigest()}.
     */
    public final String name() {
        if (name != null) {
            return name;
        }

        return name = name0().intern();
    }

    /**
     * Get function name. This name used for equality checking and in {@link RelNode#getDigest()}.
     */
    protected String name0() {
        return type().shortName;
    }

    /** {@inheritDoc} */
    @Override
    public final int hashCode() {
        return Objects.hashCode(name());
    }

    /** {@inheritDoc} */
    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DistributionFunction) { //noinspection StringEquality
            return name() == ((DistributionFunction) obj).name(); // NOPMD.UseEqualsToCompareStrings
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public final String toString() {
        return name();
    }

    public static DistributionFunction any() {
        return AnyDistribution.INSTANCE;
    }

    public static DistributionFunction broadcast() {
        return BroadcastDistribution.INSTANCE;
    }

    public static DistributionFunction singleton() {
        return SingletonDistribution.INSTANCE;
    }

    public static DistributionFunction random() {
        return RandomDistribution.INSTANCE;
    }

    public static DistributionFunction hash() {
        return HashDistribution.INSTANCE;
    }

    public static DistributionFunction identity() {
        return IdentityDistribution.INSTANCE;
    }

    private static final class AnyDistribution extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new AnyDistribution();

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.ANY;
        }

    }

    private static final class BroadcastDistribution extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new BroadcastDistribution();

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.BROADCAST_DISTRIBUTED;
        }

    }

    private static final class RandomDistribution extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new RandomDistribution();

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.RANDOM_DISTRIBUTED;
        }

    }

    private static final class SingletonDistribution extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new SingletonDistribution();

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.SINGLETON;
        }

    }

    private static class HashDistribution extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new HashDistribution();

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.HASH_DISTRIBUTED;
        }
    }

    /**
     * Distribution function, which treats column's raw value as a destination.
     */
    public static final class IdentityDistribution extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new IdentityDistribution();

        /** {@inheritDoc} */
        @Override
        public Type type() {
            return Type.HASH_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override
        protected String name0() {
            return "identity";
        }
    }
}
