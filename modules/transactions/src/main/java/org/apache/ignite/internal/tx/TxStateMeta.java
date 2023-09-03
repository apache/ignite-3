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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction state meta.
 */
public class TxStateMeta {
    private final TxState txState;

    private final String txCoordinatorId;

    private final HybridTimestamp commitTimestamp;

    private final CompletableFuture fut;

    /**
     * Constructor.
     *
     * @param txState Transaction state.
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitTimestamp Commit timestamp.
     * @param fut The future is complete when the transaction state is final.
     */
    public TxStateMeta(
            TxState txState,
            String txCoordinatorId,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable CompletableFuture<Void> fut
    ) {
        this.txState = txState;
        this.txCoordinatorId = txCoordinatorId;
        this.commitTimestamp = commitTimestamp;
        this.fut = fut == null ? new CompletableFuture() : fut;

        if (txState == TxState.COMMITED || txState == TxState.ABORTED) {
            this.fut.complete(null);
        }
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

    public CompletableFuture getFut() {
        return fut;
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
