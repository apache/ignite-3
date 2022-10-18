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
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.message.TablePartitionId;
import org.apache.ignite.internal.tostring.S;

/** Transaction meta. */
public class TxMeta implements Serializable {
    /** Serial version UID. */
    private static final long serialVersionUID = -172513482743911860L;

    /** Tx state. */
    private final TxState txState;

    /** The list of enlisted partitions. */
    private final List<TablePartitionId> enlistedPartitions;

    /** Commit timestamp. */
    private final HybridTimestamp commitTimestamp;

    /**
     * The constructor.
     *
     * @param txState Tx state.
     * @param enlistedPartitions The list of enlisted partitions.
     * @param commitTimestamp Commit timestamp.
     */
    public TxMeta(TxState txState, List<TablePartitionId> enlistedPartitions, HybridTimestamp commitTimestamp) {
        this.txState = txState;
        this.enlistedPartitions = enlistedPartitions;
        this.commitTimestamp = commitTimestamp;
    }

    public TxState txState() {
        return txState;
    }

    public List<TablePartitionId> enlistedPartitions() {
        return unmodifiableList(enlistedPartitions);
    }

    public HybridTimestamp commitTimestamp() {
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

        TxMeta txMeta = (TxMeta) o;

        if (txState != txMeta.txState) {
            return false;
        }
        if (enlistedPartitions != null ? !enlistedPartitions.equals(txMeta.enlistedPartitions) : txMeta.enlistedPartitions != null) {
            return false;
        }
        return commitTimestamp != null ? commitTimestamp.equals(txMeta.commitTimestamp) : txMeta.commitTimestamp == null;
    }

    @Override
    public int hashCode() {
        int result = txState != null ? txState.hashCode() : 0;
        result = 31 * result + (enlistedPartitions != null ? enlistedPartitions.hashCode() : 0);
        result = 31 * result + (commitTimestamp != null ? commitTimestamp.hashCode() : 0);
        return result;
    }
}
