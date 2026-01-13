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

package org.apache.ignite.internal.tx.message;

import static org.apache.ignite.internal.tx.message.TxMessageGroup.TX_STATE_META_UNKNOWN_MESSAGE;

import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMetaUnknown;

/**
 * Message for transferring a {@link TxStateMetaUnknown}.
 */
@Transferable(TX_STATE_META_UNKNOWN_MESSAGE)
public interface TxStateMetaUnknownMessage extends TxStateMetaMessage {
    /**
     * Whether write intent is readable (committed on primary replica). It is needed when the transaction state is unrecoverable
     * and only write intent state is known. Transaction state should be {@link TxState#UNKNOWN} in this case.
     *
     * @return Whether write intent is readable.
     */
    boolean writeIntentReadable();

    default TransactionMeta asTxStateMetaUnknown() {
        return new TxStateMetaUnknown(writeIntentReadable());
    }

    @Override
    default TransactionMeta asTransactionMeta() {
        return asTxStateMetaUnknown();
    }
}
