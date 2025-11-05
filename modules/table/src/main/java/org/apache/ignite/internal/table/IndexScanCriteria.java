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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.jetbrains.annotations.Nullable;

/**
 * Index scan criteria for range scan and lookup index operations.
 */
public interface IndexScanCriteria {
    /** Creates range scan criteria. */
    static Range range(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return new Range(lowerBound, upperBound, flags);
    }

    /** Creates lookup criteria. */
    static Lookup lookup(BinaryTuple key) {
        return new Lookup(key);
    }

    /** Shortcut to create unbounded range scan criteria. */
    static Range unbounded() {
        return Range.UNBOUNDED;
    }

    /**
     * Range scan criteria.
     */
    class Range implements IndexScanCriteria {
        private static final Range UNBOUNDED = new Range(null, null, 0);

        private final @Nullable BinaryTuplePrefix lowerBound;
        private final @Nullable BinaryTuplePrefix upperBound;
        private final int flags;

        private Range(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.flags = flags;
        }

        /** Get lower bound. */
        public @Nullable BinaryTuplePrefix lowerBound() {
            return lowerBound;
        }

        /** Get upper bound. */
        public @Nullable BinaryTuplePrefix upperBound() {
            return upperBound;
        }

        /** Get flags. */
        public int flags() {
            return flags;
        }
    }

    /**
     * Lookup criteria.
     */
    class Lookup implements IndexScanCriteria {
        private final BinaryTuple key;

        private Lookup(BinaryTuple key) {
            this.key = key;
        }

        /** Get lookup key. */
        public BinaryTuple key() {
            return key;
        }
    }
}
