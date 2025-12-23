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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.UUID;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Util class to format txId with tx label to use in logging and exception messages.
 */
public class TransactionLogUtils {

    public static final String TX_ID_TX_LABEL = "txId={}, txLabel={}";
    public static final String TX_ID = "txId={}";

    /**
     * Formats transaction information for logging, including both txId and label if available.
     *
     * @param txId Transaction ID.
     * @param txManager Transaction manager to retrieve label.
     * @return Formatted string like "txId=..., txLabel=..." or "txId=..." if label is null.
     */
    public static String formatTxInfo(UUID txId, TxManager txManager) {
        if (txId != null) {
            TxStateMeta txMeta = txManager.stateMeta(txId);
            return formatTxInfo(txId, txMeta);
        } else {
            return format(TX_ID, "null");
        }
    }

    /**
     * Formats transaction information for logging, including both txId and label if available.
     *
     * @param txId Transaction ID.
     * @param storage VolatileTxStateMetaStorage to retrieve label.
     * @return Formatted string like "txId=..., txLabel=..." or "txId=..." if label is null.
     */
    public static String formatTxInfo(UUID txId, VolatileTxStateMetaStorage storage) {
        if (txId != null) {
            TxStateMeta txMeta = storage.state(txId);
            String label = txMeta != null ? txMeta.txLabel() : null;
            return formatTxInfo(txId, label);
        } else {
            return format(TX_ID, "null");
        }
    }

    /**
     * Formats transaction information for logging.
     *
     * @param txId Transaction ID.
     * @param txLabel Transaction label (can be null).
     * @return Formatted string.
     */
    private static String formatTxInfo(UUID txId, @Nullable String txLabel) {
        if (txLabel != null && !txLabel.isEmpty()) {
            return format(TX_ID_TX_LABEL, txId, txLabel);
        } else {
            return format(TX_ID, txId);
        }
    }

    /**
     * Formats transaction information for logging, including both txId and label if available.
     *
     * @param txId Transaction ID.
     * @param txMeta TxStateMeta to retrieve label.
     * @return Formatted string like "txId=..., txLabel=..." or "txId=..." if label is null.
     */
    private static String formatTxInfo(UUID txId, @Nullable TxStateMeta txMeta) {
        String label = txMeta != null ? txMeta.txLabel() : null;
        return formatTxInfo(txId, label);
    }
}
