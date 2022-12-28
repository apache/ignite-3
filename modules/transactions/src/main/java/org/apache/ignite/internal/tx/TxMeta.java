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

package org.apache.ignite.internal.tx;

import static java.util.Collections.unmodifiableList;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Transaction meta. */
public class TxMeta implements Serializable {
    /** Serial version UID. */
    private static final long serialVersionUID = -172513482743911860L;

    /** Tx state. */
    private final TxState txState;

    /** The list of enlisted partitions. */
    private final List<ReplicationGroupId> enlistedPartitions;

    /** Commit timestamp. */
    @Nullable
    private final HybridTimestamp commitTimestamp;

    /**
     * The constructor.
     *
     * @param txState Tx state.
     * @param enlistedPartitions The list of enlisted partitions.
     * @param commitTimestamp Commit timestamp.
     */
    public TxMeta(TxState txState, List<ReplicationGroupId> enlistedPartitions, @Nullable HybridTimestamp commitTimestamp) {
        this.txState = txState;
        this.enlistedPartitions = enlistedPartitions;
        this.commitTimestamp = commitTimestamp;
    }

    public TxState txState() {
        return txState;
    }

    public List<ReplicationGroupId> enlistedPartitions() {
        return unmodifiableList(enlistedPartitions);
    }

    public @Nullable HybridTimestamp commitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public String toString() {
        return S.toString(TxMeta.class, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TxMeta other = (TxMeta) o;

        return txState == other.txState
                && enlistedPartitions.equals(other.enlistedPartitions)
                && Objects.equals(commitTimestamp, other.commitTimestamp);
    }

    @Override
    public int hashCode() {
        int result = txState.hashCode();
        result = 31 * result + enlistedPartitions.hashCode();
        result = 31 * result + (commitTimestamp != null ? commitTimestamp.hashCode() : 0);
        return result;
    }
}
