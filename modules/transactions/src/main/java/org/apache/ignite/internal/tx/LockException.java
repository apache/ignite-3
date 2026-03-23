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

import org.apache.ignite.tx.RetriableTransactionException;

/**
 * This exception is thrown when a lock cannot be acquired, released or downgraded.
 */
public class LockException extends TransactionInternalCheckedException implements RetriableTransactionException {
    /**
     * Creates a new instance of LockException with the given message.
     *
     * @param code Full error code. {{@link org.apache.ignite.lang.ErrorGroups.Transactions#ACQUIRE_LOCK_ERR},
     *     {@link org.apache.ignite.lang.ErrorGroups.Transactions#ACQUIRE_LOCK_TIMEOUT_ERR},
     * @param msg The detail message.
     */
    public LockException(int code, String msg) {
        super(code, msg);
    }

    /**
     * Creates a new instance of LockException with the given message.
     *
     * @param code Full error code. {{@link org.apache.ignite.lang.ErrorGroups.Transactions#ACQUIRE_LOCK_ERR},
     *     {@link org.apache.ignite.lang.ErrorGroups.Transactions#ACQUIRE_LOCK_TIMEOUT_ERR},
     * @param msg The detail message.
     * @param cause Cause of the exception.
     */
    protected LockException(int code, String msg, Throwable cause) {
        super(code, msg, cause);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        // Omits redundant stacktrace.
        return this;
    }
}
