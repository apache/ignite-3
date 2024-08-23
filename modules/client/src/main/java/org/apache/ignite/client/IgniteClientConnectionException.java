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

package org.apache.ignite.client;

import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Indicates all the Ignite servers specified in the client configuration are no longer available.
 */
public class IgniteClientConnectionException extends IgniteException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** The endpoint that caused the exception. */
    private final @Nullable String endpoint;

    /**
     * Constructs a new exception with the specified cause and detail message.
     *
     * @param code  the error code.
     * @param msg   the detail message.
     * @param cause the cause.
     */
    public IgniteClientConnectionException(int code, String msg, @Nullable String endpoint, @Nullable Throwable cause) {
        super(code, getMessage(msg, endpoint), cause);

        this.endpoint = endpoint;
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param code  the error code.
     * @param msg   the detail message.
     */
    public IgniteClientConnectionException(int code, String msg, @Nullable String endpoint) {
        this(code, msg, endpoint, null);
    }

    /**
     * Returns the endpoint that caused the exception.
     *
     * @return the endpoint that caused the exception.
     */
    public @Nullable String endpoint() {
        return endpoint;
    }

    private static @NotNull String getMessage(String msg, String endpoint) {
        return endpoint == null || endpoint.isEmpty() ? msg : msg + " [endpoint=" + endpoint + "]";
    }
}
