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

package org.apache.ignite.internal.tx.message;

import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.network.annotations.Marshallable;
import org.apache.ignite.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction state response.
 */
@Transferable(TxMessageGroup.TX_STATE_RESPONSE)
public interface TxStateResponse extends TimestampAware {
    @Marshallable
    @Nullable
    TransactionMeta txStateMeta();
}
