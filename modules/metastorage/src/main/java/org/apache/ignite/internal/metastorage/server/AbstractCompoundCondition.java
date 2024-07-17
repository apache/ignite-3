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

package org.apache.ignite.internal.metastorage.server;

import java.util.Arrays;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.util.ArrayUtils;

/**
 * Compound condition, which {@link #combine(boolean, boolean)} results of left and right conditions.
 * Also, aggregate their keys in the left-right order.
 */
public abstract class AbstractCompoundCondition implements Condition {
    /** Left condition. */
    private final Condition leftCondition;

    /** Right condition. */
    private final Condition rightCondition;

    /** Aggregated array of keys from left and right conditions. */
    private final byte[][] keys;

    /**
     * Constructs new compound condition with aggregated keys.
     *
     * @param leftCondition left condition.
     * @param rightCondition right condition.
     */
    public AbstractCompoundCondition(Condition leftCondition, Condition rightCondition) {
        this.leftCondition = leftCondition;
        this.rightCondition = rightCondition;
        keys = ArrayUtils.concat(leftCondition.keys(), rightCondition.keys());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[][] keys() {
        return keys;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(Entry... entries) {
        return combine(
                leftCondition.test(Arrays.copyOf(entries, leftCondition.keys().length)),
                rightCondition.test(
                        Arrays.copyOfRange(entries,
                                leftCondition.keys().length,
                                leftCondition.keys().length + rightCondition.keys().length)));
    }

    /**
     * Combine boolean condition from the results of left and right conditions.
     *
     * @param left condition.
     * @param right condition.
     * @return result of compound condition.
     */
    protected abstract boolean combine(boolean left, boolean right);

    /**
     * Returns left condition.
     *
     * @return left condition.
     */
    public Condition leftCondition() {
        return leftCondition;
    }

    /**
     * Returns right condition.
     *
     * @return right condition.
     */
    public Condition rightCondition() {
        return rightCondition;
    }
}
