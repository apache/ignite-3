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

package org.apache.ignite.sql;

import java.util.UUID;
import org.apache.ignite.lang.IgniteException;

/**
 * SQL exception base class.
 */
public class SqlException extends IgniteException {
    /**
     * Creates an exception with the given error code.
     *
     * @param code Full error code.
     */
    public SqlException(int code) {
        super(code);
    }

    /**
     * Creates an exception with the given error code and detailed message.
     *
     * @param code Full error code.
     * @param message Detailed message.
     */
    public SqlException(int code, String message) {
        super(code, message);
    }

    /**
     * Creates an exception with the given trace ID, error code, and detailed message.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param message Detailed message.
     */
    public SqlException(UUID traceId, int code, String message) {
        super(traceId, code, message);
    }

    /**
     * Creates an exception with the given error code and cause.
     *
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public SqlException(int code, Throwable cause) {
        super(code, cause);
    }

    /**
     * Creates an exception with the given trace ID, error code, and cause.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public SqlException(UUID traceId, int code, Throwable cause) {
        super(traceId, code, cause);
    }

    /**
     * Creates an exception with the given error code, detailed message and cause.
     *
     * @param code Full error code.
     * @param message Detailed message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public SqlException(int code, String message, Throwable cause) {
        super(code, message, cause);
    }

    /**
     * Creates an exception with the given trace ID, error code, detailed message, and cause.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param message Detailed message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public SqlException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
