/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.tx;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.util.Collections.unmodifiableList;

public class TxMeta implements Serializable {
    private final TxState txState;

    private final List<IgniteBiTuple<Integer, Integer>> enlistedPartitions;

    private final long commitTimestamp;

    public TxMeta(TxState txState, List<IgniteBiTuple<Integer, Integer>> enlistedPartitions, long commitTimestamp) {
        this.txState = txState;
        this.enlistedPartitions = enlistedPartitions;
        this.commitTimestamp = commitTimestamp;
    }

    public TxState txState() {
        return txState;
    }

    public List<IgniteBiTuple<Integer, Integer>> enlistedPartitions() {
        return unmodifiableList(enlistedPartitions);
    }

    public long commitTimestamp() {
        return commitTimestamp;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TxMeta meta = (TxMeta)o;
        return commitTimestamp == meta.commitTimestamp
            && txState == meta.txState
            && Objects.equals(enlistedPartitions, meta.enlistedPartitions);
    }

    @Override public int hashCode() {
        return Objects.hash(txState, enlistedPartitions, commitTimestamp);
    }
}
