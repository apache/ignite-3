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

package org.apache.ignite.internal.storage;

import java.util.UUID;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.IgniteException;

/**
 * Exception class that describes the situation when two independent transactions attempt to write values for the same key.
 */
public class TxIdMismatchException extends IgniteException {
    /** Expected transaction id. */
    private final UUID expectedTransactionId;

    /** Conflicting transaction id. */
    private final UUID conflictingTransactionId;

    /**
     * Constructor.
     *
     * @param expectedTxId Expected transaction id.
     * @param conflictingTxId Conflicting transaction id.
     */
    public TxIdMismatchException(UUID expectedTxId, UUID conflictingTxId) {
        super(S.toString(
                "Mismatched transaction id",
                "expectedTxId", expectedTxId, false,
                "actualTxId", conflictingTxId, false
        ));

        expectedTransactionId = expectedTxId;
        conflictingTransactionId = conflictingTxId;
    }

    /**
     * Returns an expected transaction id.
     */
    public UUID expectedTransactionId() {
        return expectedTransactionId;
    }

    /**
     * Returns a conflicting transaction id.
     */
    public UUID conflictingTransactionId() {
        return conflictingTransactionId;
    }
}
