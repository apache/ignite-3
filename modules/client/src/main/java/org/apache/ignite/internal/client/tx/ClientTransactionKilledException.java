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

package org.apache.ignite.internal.client.tx;

import java.util.UUID;
import org.apache.ignite.tx.RetriableTransactionException;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Reports a killed transaction.
 */
public class ClientTransactionKilledException extends TransactionException implements RetriableTransactionException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Transaction id. */
    private final UUID txId;

    /**
     * Constructor.
     *
     * @param traceId Trace ID.
     * @param code Error code.
     * @param message String message.
     * @param txId Related transaction id.
     * @param cause The cause.
     */
    public ClientTransactionKilledException(UUID traceId, int code, @Nullable String message, UUID txId, @Nullable Throwable cause) {
        super(traceId, code, message, cause);

        this.txId = txId;
    }

    /**
     * Constructor (for copying purposes).
     *
     * @param traceId Trace ID.
     * @param code Error code.
     * @param message String message.
     * @param cause The cause.
     */
    public ClientTransactionKilledException(UUID traceId, int code, @Nullable String message, @Nullable Throwable cause) {
        super(traceId, code, message, cause);

        if (cause instanceof ClientTransactionKilledException) {
            this.txId = ((ClientTransactionKilledException) cause).txId;
        } else {
            throw new IllegalArgumentException("Copy constructor should only be called with the source exception");
        }
    }

    /**
     * Returns a related transaction id.
     *
     * @return The id.
     */
    public UUID txId() {
        return txId;
    }
}
