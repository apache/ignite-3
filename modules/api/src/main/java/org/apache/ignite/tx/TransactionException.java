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
import org.apache.ignite.lang.IgniteException;

/** This exception is thrown if a transaction can't be finished by some reasons. */
public class TransactionException extends IgniteException {
    /**
     * Creates a new transaction exception with a message.
     *
     * @param message The message.
     */
    @Deprecated
    public TransactionException(String message) {
        super(message);
    }

    /**
     * Creates a new transaction exception with a cause.
     *
     * @param cause The cause.
     */
    @Deprecated
    public TransactionException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new transaction exception with the given error code and cause.
     *
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TransactionException(int code, Throwable cause) {
        super(code, cause);
    }

    /**
     * Creates a new transaction exception with the given error code and detail message.
     *
     * @param code Full error code.
     * @param message Detail message.
     */
    public TransactionException(int code, String message) {
        super(code, message);
    }


    /**
     * Creates a new transaction exception with the given trace id, error code and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TransactionException(UUID traceId, int code, Throwable cause) {
        super(traceId, code, cause);
    }

    /**
     * Creates a new transaction exception with the given error code, detail message and cause.
     *
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TransactionException(int code, String message, Throwable cause) {
        super(code, message, cause);
    }

    /**
     * Creates a new transaction exception with the given trace id, error code, detail message and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TransactionException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
