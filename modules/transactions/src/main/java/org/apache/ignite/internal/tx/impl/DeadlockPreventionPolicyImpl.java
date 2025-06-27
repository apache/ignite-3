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

package org.apache.ignite.internal.tx.impl;

import java.util.Comparator;
import java.util.UUID;
import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link DeadlockPreventionPolicy} that allows to set its parameters directly in the constructor, so it is able to
 * provide different policies' behaviour such as wait-die, reversed wait-die, timeout wait, no-wait, etc.
 */
public class DeadlockPreventionPolicyImpl implements DeadlockPreventionPolicy {
    private final Comparator<UUID> txIdComparator;

    private final long waitTimeout;

    /**
     * Constructor.
     *
     * @param txIdComparator Comparator name as {@link TxIdComparators} element.
     * @param waitTimeout Wait timeout.
     */
    public DeadlockPreventionPolicyImpl(TxIdComparators txIdComparator, long waitTimeout) {
        switch (txIdComparator) {
            case NATURAL: {
                this.txIdComparator = new TxIdPriorityComparator();
                break;
            }
            case REVERSED: {
                this.txIdComparator = new TxIdPriorityComparator().reversed();
                break;
            }
            case NONE: {
                this.txIdComparator = null;
                break;
            }
            default: {
                throw new IllegalArgumentException("Unknown comparator type: " + txIdComparator);
            }
        }

        this.waitTimeout = waitTimeout;
    }

    @Override
    public @Nullable Comparator<UUID> txIdComparator() {
        return txIdComparator;
    }

    @Override
    public long waitTimeout() {
        return waitTimeout;
    }

    /**
     * Enum of names of transaction ID comparators.
     */
    public enum TxIdComparators {
        NATURAL,
        REVERSED,
        NONE
    }
}
