/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.lang;

import static org.apache.ignite.lang.ErrorGroup.ERR_PREFIX;
import static org.apache.ignite.lang.ErrorGroup.errorGroupByCode;
import static org.apache.ignite.lang.ErrorGroup.errorMessage;
import static org.apache.ignite.lang.ErrorGroup.errorMessageFromCause;
import static org.apache.ignite.lang.ErrorGroup.extractErrorCode;
import static org.apache.ignite.lang.ErrorGroup.extractGroupCode;

import java.util.UUID;

/**
 * General Ignite exception. This exception is used to indicate any error condition within the node.
 */
public class IgniteCheckedException extends Exception {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Name of the error group. */
    private final String groupName;

    /**
     * Error code which contains information about error group and code, where code is unique within the group.
     * The structure of a code is shown in the following diagram:
     * +------------+--------------+
     * |  16 bits   |    16 bits   |
     * +------------+--------------+
     * | Group Code |  Error Code  |
     * +------------+--------------+
     */
    private final int code;

    /** Unique identifier of this exception that should help locating the error message in a log file. */
    private final UUID traceId;

    /**
     * Creates a new exception with the given error code.
     *
     * @param code Full error code.
     */
    public IgniteCheckedException(int code) {
        this(UUID.randomUUID(), code);
    }

    /**
     * Creates a new exception with the given trace id and error code.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     */
    public IgniteCheckedException(UUID traceId, int code) {
        super(errorMessage(traceId, code, null));

        this.traceId = traceId;
        this.groupName = errorGroupByCode((extractGroupCode(code))).name();
        this.code = code;
    }

    /**
     * Creates a new exception with the given error code and detail message.
     *
     * @param code Full error code.
     * @param message Detail message.
     */
    public IgniteCheckedException(int code, String message) {
        this(UUID.randomUUID(), code, message);
    }

    /**
     * Creates a new exception with the given trace id, error code and detail message.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Detail message.
     */
    public IgniteCheckedException(UUID traceId, int code, String message) {
        super(errorMessage(traceId, code, message));

        this.traceId = traceId;
        this.groupName = errorGroupByCode((extractGroupCode(code))).name();
        this.code = code;
    }

    /**
     * Creates a new exception with the given error code and cause.
     *
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteCheckedException(int code, Throwable cause) {
        this(UUID.randomUUID(), code, cause);
    }

    /**
     * Creates a new exception with the given trace id, error code and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteCheckedException(UUID traceId, int code, Throwable cause) {
        super(errorMessageFromCause(traceId, code, cause), cause);

        this.traceId = traceId;
        this.groupName = errorGroupByCode((extractGroupCode(code))).name();
        this.code = code;
    }

    /**
     * Creates a new exception with the given error code, detail message and cause.
     *
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteCheckedException(int code, String message, Throwable cause) {
        this(UUID.randomUUID(), code, message, cause);
    }

    /**
     * Creates a new exception with the given trace id, error code, detail message and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteCheckedException(UUID traceId, int code, String message, Throwable cause) {
        super(errorMessage(traceId, code, message), cause);

        this.traceId = traceId;
        this.groupName = errorGroupByCode((extractGroupCode(code))).name();
        this.code = code;
    }

    /**
     * Returns a group name of this error.
     *
     * @see #groupCode()
     * @see #code()
     * @return Group name.
     */
    public String groupName() {
        return groupName;
    }

    /**
     * Returns a full error code which includes a group of the error and code which is uniquely identifies a problem within the group.
     * This is a combination of two most-significant bytes that represent the error group and
     * two least-significant bytes for the error code.
     *
     * @return Full error code.
     */
    public int code() {
        return code;
    }

    /**
     * Returns a human-readable string represents a full error code.
     * Returned string has the following format: IGN-XXX-nnn, where XXX is a group name and nnn is an unique error code within a group.
     *
     * @return Full error code in a human-readable format.
     */
    public String codeAsString() {
        return ERR_PREFIX + groupName() + '-' + errorCode();
    }

    /**
     * Returns error group.
     *
     * @see #code()
     * @return Error group.
     */
    public int groupCode() {
        return extractGroupCode(code);
    }

    /**
     * Returns error code that uniquely identifies a problem within a group.
     *
     * @see #code()
     * @see #groupCode()
     * @return Error code.
     */
    public int errorCode() {
        return extractErrorCode(code);
    }

    /**
     * Returns an unique identifier of this exception.
     *
     * @return Unique identifier of this exception.
     */
    public UUID traceId() {
        return traceId;
    }
}
