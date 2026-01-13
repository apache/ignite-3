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

import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateMetaMessage;

/**
 * Unknown transaction state meta.
 */
public class TxStateMetaUnknown extends TxStateMeta {
    private static final long serialVersionUID = -2549857341422406570L;

    private final boolean writeIntentReadable;

    public TxStateMetaUnknown(boolean writeIntentReadable) {
        super(TxState.UNKNOWN, null, null, null, null, null);

        this.writeIntentReadable = writeIntentReadable;
    }

    public static TxStateMetaUnknown txStateMetaUnknown(boolean writeIntentReadable) {
        return new TxStateMetaUnknown(writeIntentReadable);
    }

    @Override
    public TxStateMetaMessage toTransactionMetaMessage(ReplicaMessagesFactory replicaMessagesFactory, TxMessagesFactory txMessagesFactory) {
        return txMessagesFactory
                .txStateMetaUnknownMessage()
                .writeIntentReadable(writeIntentReadable)
                .build();
    }
}
