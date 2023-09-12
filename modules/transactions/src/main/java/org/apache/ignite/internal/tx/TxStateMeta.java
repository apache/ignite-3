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

import java.io.Serializable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction state meta.
 */
public class TxStateMeta implements Serializable {
    private static final long serialVersionUID = 8521181896862227127L;

    private final TxState txState;

    private final String txCoordinatorId;

    private final HybridTimestamp commitTimestamp;

    /**
     * Constructor.
     *
     * @param txState Transaction state.
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitTimestamp Commit timestamp.
     */
    public TxStateMeta(
            TxState txState,
            String txCoordinatorId,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        this.txState = txState;
        this.txCoordinatorId = txCoordinatorId;
        this.commitTimestamp = commitTimestamp;
    }

    public TxState txState() {
        return txState;
    }

    public String txCoordinatorId() {
        return txCoordinatorId;
    }

    public @Nullable HybridTimestamp commitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TxStateMeta that = (TxStateMeta) o;

        if (txState != that.txState) {
            return false;
        }
        if (txCoordinatorId != null ? !txCoordinatorId.equals(that.txCoordinatorId) : that.txCoordinatorId != null) {
            return false;
        }
        return commitTimestamp != null ? commitTimestamp.equals(that.commitTimestamp) : that.commitTimestamp == null;
    }

    @Override
    public int hashCode() {
        int result = txState != null ? txState.hashCode() : 0;
        result = 31 * result + (txCoordinatorId != null ? txCoordinatorId.hashCode() : 0);
        result = 31 * result + (commitTimestamp != null ? commitTimestamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "[txState=" + txState + ", txCoordinatorId=" + txCoordinatorId + ", commitTimestamp=" + commitTimestamp + ']';
    }
}
