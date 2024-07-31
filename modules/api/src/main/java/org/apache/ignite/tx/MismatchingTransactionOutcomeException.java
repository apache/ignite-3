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

package org.apache.ignite.tx;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * The exception is thrown when the transaction result differs from the intended one.
 *
 * <p>For example, {@code tx.commit()} is called for a transaction, but the verification logic decided to abort it instead. The transaction
 * will be aborted and the call to {@code tx.commit()} will throw this exception.</p>
 */
public class MismatchingTransactionOutcomeException extends TransactionException {
    /**
     * Constructs a new instance.
     *
     * @param traceId Trace ID.
     * @param code Full error code.
     * @param message Error message.
     * @param cause The Throwable that is the cause of this exception (can be {@code null}).
     */
    public MismatchingTransactionOutcomeException(UUID traceId, int code, String message, @Nullable Throwable cause) {
        super(traceId, code, message, cause);
    }
}
