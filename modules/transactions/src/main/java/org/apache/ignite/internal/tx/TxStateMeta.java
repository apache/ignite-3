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

import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Transaction state meta.
 */
public class TxStateMeta {
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
    public TxStateMeta(TxState txState, String txCoordinatorId, HybridTimestamp commitTimestamp) {
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

    public HybridTimestamp commitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public String toString() {
        return "[txState=" + txState + ", txCoordinatorId=" + txCoordinatorId + ", commitTimestamp=" + commitTimestamp + ']';
    }
}
