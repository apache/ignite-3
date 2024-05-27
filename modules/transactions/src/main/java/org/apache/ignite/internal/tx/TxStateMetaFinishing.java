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
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TxStateMeta} implementation for {@link TxState#FINISHING} state. Contains future that is is completed after the state of
 * corresponding transaction changes to final state.
 */
public class TxStateMetaFinishing extends TxStateMeta {
    private static final long serialVersionUID = 9122953981654023665L;

    /** Future that is completed after the state of corresponding transaction changes to final state. */
    private final CompletableFuture<TransactionMeta> txFinishFuture = new CompletableFuture<>();

    /**
     * Constructor.
     *
     * @param txCoordinatorId Transaction coordinator id.
     * @param commitPartitionId Commit partition id.
     */
    public TxStateMetaFinishing(@Nullable String txCoordinatorId, @Nullable ZonePartitionId commitPartitionId) {
        super(TxState.FINISHING, txCoordinatorId, commitPartitionId, null);
    }

    /**
     * Future that is completed after the state of corresponding transaction changes to final state.
     *
     * @return Future that is completed after the state of corresponding transaction changes to final state.
     */
    public CompletableFuture<TransactionMeta> txFinishFuture() {
        return txFinishFuture;
    }

    @Override
    public @Nullable HybridTimestamp commitTimestamp() {
        throw new UnsupportedOperationException("Can't get commit timestamp from FINISHING transaction state meta.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        TxStateMetaFinishing that = (TxStateMetaFinishing) o;

        return txFinishFuture.equals(that.txFinishFuture);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();

        result = 31 * result + txFinishFuture.hashCode();

        return result;
    }
}
