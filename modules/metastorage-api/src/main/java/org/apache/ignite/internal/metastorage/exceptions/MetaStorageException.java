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

package org.apache.ignite.internal.metastorage.exceptions;

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * Base class for meta storage exceptions.
 */
public class MetaStorageException extends IgniteInternalException {
    /**
     * Creates a new exception with the given error code.
     *
     * @param code Full error code.
     */
    public MetaStorageException(int code) {
        super(code);
    }

    /**
     * Creates a new exception with the given trace id and error code.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     */
    public MetaStorageException(UUID traceId, int code) {
        super(traceId, code);
    }

    /**
     * Creates a new exception with the given error code and detail message.
     *
     * @param code Full error code.
     * @param message Detail message.
     */
    public MetaStorageException(int code, String message) {
        super(code, message);
    }

    /**
     * Creates a new exception with the given trace id, error code and detail message.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Detail message.
     */
    public MetaStorageException(UUID traceId, int code, String message) {
        super(traceId, code, message);
    }

    /**
     * Creates a new exception with the given error code and cause.
     *
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public MetaStorageException(int code, Throwable cause) {
        super(code, cause);
    }

    /**
     * Creates a new exception with the given trace id, error code and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public MetaStorageException(UUID traceId, int code, Throwable cause) {
        super(traceId, code, cause);
    }

    /**
     * Creates a new exception with the given error code, detail message and cause.
     *
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public MetaStorageException(int code, String message, Throwable cause) {
        super(code, message, cause);
    }
}
