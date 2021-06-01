/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.trait;

import java.util.Objects;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;

import static org.apache.ignite.internal.processors.query.calcite.Stubs.intFoo;

/**
 * Distribution function.
 */
public abstract class DistributionFunction {
    /** */
    private String name;

    /** */
    private DistributionFunction() {
        // No-op.
    }

    /**
     * @return Distribution function type.
     */
    public abstract RelDistribution.Type type();

    /**
     * @return Function name. This name used for equality checking and in {@link RelNode#getDigest()}.
     */
    public final String name() {
        if (name != null)
            return name;

        return name = name0().intern();
    }

    /** */
    public boolean affinity() {
        return false;
    }

    /** */
    public int cacheId() {
        return intFoo()/*CU.UNDEFINED_CACHE_ID*/;
    }

    /**
     * @return Function name. This name used for equality checking and in {@link RelNode#getDigest()}.
     */
    protected String name0() {
        return type().shortName;
    }

    /** {@inheritDoc} */
    @Override public final int hashCode() {
        return Objects.hashCode(name());
    }

    /** {@inheritDoc} */
    @Override public final boolean equals(Object obj) {
        if (obj instanceof DistributionFunction)
            //noinspection StringEquality
            return name() == ((DistributionFunction) obj).name();

        return false;
    }

    /** {@inheritDoc} */
    @Override public final String toString() {
        return name();
    }

    /** */
    public static DistributionFunction any() {
        return AnyDistribution.INSTANCE;
    }

    /** */
    public static DistributionFunction broadcast() {
        return BroadcastDistribution.INSTANCE;
    }

    /** */
    public static DistributionFunction singleton() {
        return SingletonDistribution.INSTANCE;
    }

    /** */
    public static DistributionFunction random() {
        return RandomDistribution.INSTANCE;
    }

    /** */
    public static DistributionFunction hash() {
        return HashDistribution.INSTANCE;
    }

    /** */
    public static DistributionFunction affinity(int cacheId, Object identity) {
        return new AffinityDistribution(cacheId, identity);
    }

    /** */
    public static boolean satisfy(DistributionFunction f0, DistributionFunction f1) {
        if (f0 == f1 || f0.name() == f1.name())
            return true;

        return f0 instanceof AffinityDistribution && f1 instanceof AffinityDistribution &&
            Objects.equals(((AffinityDistribution)f0).identity(), ((AffinityDistribution)f1).identity());
    }

    /** */
    private static final class AnyDistribution extends DistributionFunction {
        /** */
        public static final DistributionFunction INSTANCE = new AnyDistribution();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.ANY;
        }
    }

    /** */
    private static final class BroadcastDistribution extends DistributionFunction {
        /** */
        public static final DistributionFunction INSTANCE = new BroadcastDistribution();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.BROADCAST_DISTRIBUTED;
        }
    }

    /** */
    private static final class RandomDistribution extends DistributionFunction {
        /** */
        public static final DistributionFunction INSTANCE = new RandomDistribution();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.RANDOM_DISTRIBUTED;
        }
    }

    /** */
    private static final class SingletonDistribution extends DistributionFunction {
        /** */
        public static final DistributionFunction INSTANCE = new SingletonDistribution();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.SINGLETON;
        }
    }

    /** */
    private static final class HashDistribution extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new HashDistribution();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.HASH_DISTRIBUTED;
        }
    }

    /** */
    private static final class AffinityDistribution extends DistributionFunction {
        /** */
        private final int cacheId;

        /** */
        private final Object identity;

        /**
         * @param cacheId Cache ID.
         * @param identity Affinity identity key.
         */
        public AffinityDistribution(int cacheId, Object identity) {
            this.cacheId = cacheId;
            this.identity = identity;
        }

        /** {@inheritDoc} */
        @Override public boolean affinity() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public int cacheId() {
            return cacheId;
        }

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.HASH_DISTRIBUTED;
        }

        /** */
        public Object identity() {
            return identity;
        }

        /** {@inheritDoc} */
        @Override protected String name0() {
            return "affinity[identity=" + identity + ", cacheId=" + cacheId + ']';
        }
    }
}
