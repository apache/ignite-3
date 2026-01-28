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

    private static final String TX_ID_TX_LABEL = "txId={}, txLabel={}";
    private static final String TX_ID = "txId={}";

    /**
     * Formats transaction information for logging, including both txId and label if available.
     *
     * @param txId Transaction ID.
     * @param txManager Transaction manager to retrieve label.
     * @return Formatted string like "txId=..., txLabel=..." or "txId=..." if label is null.
     */
    public static String formatTxInfo(UUID txId, TxManager txManager) {
        return formatTxInfo(txId, txManager, true);
    }

    /**
     * Formats transaction information for logging, including both txId and label if available.
     *
     * @param txId Transaction ID.
     * @param txManager Transaction manager to retrieve label.
     * @param wrapped Whether to wrap formatted values into square brackets ({@code [...]}). If {@code false}, the result contains the same
     *     key-value pairs but without square brackets.
     * @return Formatted string.
     */
    public static String formatTxInfo(UUID txId, TxManager txManager, boolean wrapped) {
        if (txId != null) {
            TxStateMeta txMeta = txManager.stateMeta(txId);
            return formatTxInfo(txId, txMeta, wrapped);
        } else {
            String base = format(TX_ID, "null");
            return wrapped ? "[" + base + "]" : base;
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
        return formatTxInfo(txId, storage, true);
    }

    /**
     * Formats transaction information for logging, including both txId and label if available.
     *
     * @param txId Transaction ID.
     * @param storage VolatileTxStateMetaStorage to retrieve label.
     * @param wrapped Whether to wrap formatted values into square brackets ({@code [...]}). If {@code false}, the result contains the same
     *     key-value pairs but without square brackets.
     * @return Formatted string.
     */
    public static String formatTxInfo(UUID txId, VolatileTxStateMetaStorage storage, boolean wrapped) {
        if (txId != null) {
            TxStateMeta txMeta = storage.state(txId);
            String label = txMeta != null ? txMeta.txLabel() : null;
            return formatTxInfo(txId, label, wrapped);
        } else {
            String base = format(TX_ID, "null");
            return wrapped ? "[" + base + "]" : base;
        }
    }

    /**
     * Formats transaction information for logging.
     *
     * @param txId Transaction ID.
     * @param txLabel Transaction label (can be null).
     * @return Formatted string.
     */
    private static String formatTxInfo(UUID txId, @Nullable String txLabel, boolean wrapped) {
        String base;

        if (txLabel != null && !txLabel.isEmpty()) {
            base = format(TX_ID_TX_LABEL, txId, txLabel);
        } else {
            base = format(TX_ID, txId);
        }

        return wrapped ? "[" + base + "]" : base;
    }

    /**
     * Formats transaction information for logging, including both txId and label if available.
     *
     * @param txId Transaction ID.
     * @param txMeta TxStateMeta to retrieve label.
     * @return Formatted string like "txId=..., txLabel=..." or "txId=..." if label is null.
     */
    private static String formatTxInfo(UUID txId, @Nullable TxStateMeta txMeta, boolean wrapped) {
        String label = txMeta != null ? txMeta.txLabel() : null;
        return formatTxInfo(txId, label, wrapped);
    }
}
