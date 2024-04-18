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

package org.apache.ignite.internal.table.distributed;

import java.util.BitSet;
import java.util.stream.IntStream;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;

/** {@link BitSet} implementation of the {@link PartitionSet}. */
public class BitSetPartitionSet implements PartitionSet {
    /** Backing BitSet. */
    @IgniteToStringExclude
    private final BitSet backingSet;

    public BitSetPartitionSet() {
        this.backingSet = new BitSet();
    }

    private BitSetPartitionSet(BitSet backingSet) {
        this.backingSet = backingSet;
    }

    @Override
    public void set(int partitionId) {
        backingSet.set(partitionId);
    }

    @Override
    public void clear(int partitionId) {
        backingSet.clear(partitionId);
    }

    @Override
    public boolean get(int partitionId) {
        return backingSet.get(partitionId);
    }

    @Override
    public IntStream stream() {
        return backingSet.stream();
    }

    @Override
    public int size() {
        return backingSet.cardinality();
    }

    @Override
    public PartitionSet copy() {
        return new BitSetPartitionSet((BitSet) backingSet.clone());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null) {
            return false;
        }

        if (!(o instanceof BitSetPartitionSet)) {
            return isEqual(o);
        }

        BitSetPartitionSet that = (BitSetPartitionSet) o;

        return backingSet.equals(that.backingSet);
    }

    @Override
    public int hashCode() {
        return getHashCode();
    }

    @Override
    public String toString() {
        return S.toString(BitSetPartitionSet.class, this, "partitionIds=", backingSet.stream().toArray());
    }
}
