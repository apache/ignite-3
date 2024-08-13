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

package org.apache.ignite.lang;

import static org.apache.ignite.lang.ErrorGroup.ERR_PREFIX;
import static org.apache.ignite.lang.ErrorGroup.errorMessage;
import static org.apache.ignite.lang.ErrorGroup.extractErrorCode;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.errorGroupByCode;
import static org.apache.ignite.lang.ErrorGroups.extractGroupCode;
import static org.apache.ignite.lang.util.TraceIdUtils.getOrCreateTraceId;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * General Ignite exception. Used to indicate any error condition within a node.
 */
public class IgniteException extends RuntimeException implements TraceableException {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Name of the error group. */
    private final String groupName;

    /**
     * Error code that contains information about the error group and code,
     * where the code is unique within the group. The code structure is as follows:
     * +------------+--------------+
     * |  16 bits   |    16 bits   |
     * +------------+--------------+
     * | Group Code |  Error Code  |
     * +------------+--------------+
     */
    private final int code;

    /**
     * Unique identifier of the exception that helps locate the error message in a log file.
     *
     * <p>Not {@code final} because it gets accessed via reflection when creating a copy.
     */
    @SuppressWarnings({"NonFinalFieldOfException", "FieldMayBeFinal"})
    private UUID traceId;

    /**
     * Creates an empty exception.
     */
    @Deprecated
    public IgniteException() {
        this(INTERNAL_ERR);
    }

    /**
     * Creates an exception with the given error message.
     *
     * @param msg Error message.
     */
    @Deprecated
    public IgniteException(String msg) {
        this(INTERNAL_ERR, msg);
    }

    /**
     * Creates a grid exception with the given throwable as a cause and source of the error message.
     *
     * @param cause Non-null throwable cause.
     */
    @Deprecated
    public IgniteException(Throwable cause) {
        this(INTERNAL_ERR, cause);
    }

    /**
     * Creates an exception with the given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    @Deprecated
    public IgniteException(String msg, @Nullable Throwable cause) {
        this(INTERNAL_ERR, msg, cause);
    }

    /**
     * Creates an exception with the given error code.
     *
     * @param code Full error code.
     */
    public IgniteException(int code) {
        this(UUID.randomUUID(), code);
    }

    /**
     * Creates an exception with the given trace ID and error code.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     */
    public IgniteException(UUID traceId, int code) {
        this.traceId = traceId;
        this.groupName = errorGroupByCode(code).name();
        this.code = code;
    }

    /**
     * Creates an exception with the given error code and detailed message.
     *
     * @param code Full error code.
     * @param message Detailed message.
     */
    public IgniteException(int code, String message) {
        this(UUID.randomUUID(), code, message);
    }

    /**
     * Creates an exception with the given error code and detailed message with the ability to skip filling in the stacktrace.
     *
     * @param code Full error code.
     * @param message Detailed message.
     * @param writableStackTrace Whether or not the stack trace should be writable.
     */
    public IgniteException(int code, String message, boolean writableStackTrace) {
        this(UUID.randomUUID(), code, message, null, true, writableStackTrace);
    }

    /**
     * Creates an exception with the given trace ID, error code, and detailed message.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Detailed message.
     */
    public IgniteException(UUID traceId, int code, String message) {
        this(traceId, code, message, null);
    }

    /**
     * Creates an exception with the given error code and cause.
     *
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteException(int code, @Nullable Throwable cause) {
        this(getOrCreateTraceId(cause), code, cause);
    }

    /**
     * Creates an exception with the given trace ID, error code, and cause.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteException(UUID traceId, int code, @Nullable Throwable cause) {
        super((cause != null) ? cause.getLocalizedMessage() : null, cause);

        this.traceId = traceId;
        this.groupName = errorGroupByCode(code).name();
        this.code = code;
    }

    /**
     * Creates an exception with the given error code, detailed message, and cause.
     *
     * @param code Full error code.
     * @param message Detailed message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteException(int code, String message, @Nullable Throwable cause) {
        this(getOrCreateTraceId(cause), code, message, cause);
    }

    /**
     * Creates an exception with the given trace ID, error code, detailed message, and cause.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param message Detailed message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteException(UUID traceId, int code, String message, @Nullable Throwable cause) {
        this(traceId, code, message, cause, true, true);
    }

    /**
     * Creates an exception with the given trace ID, error code, and detailed message.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Detailed message.
     * @param cause Optional nested exception (can be {@code null}).
     * @param enableSuppression Whether or not suppression is enabled or disabled.
     * @param writableStackTrace Whether or not the stack trace should be writable.
     */
    public IgniteException(
            UUID traceId,
            int code,
            String message,
            @Nullable Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace
    ) {
        super(message, cause, enableSuppression, writableStackTrace);

        this.traceId = traceId;
        this.groupName = errorGroupByCode(code).name();
        this.code = code;
    }

    /**
     * Returns a group name of the error.
     *
     * @see #groupCode()
     * @see #code()
     * @return Group name.
     */
    public String groupName() {
        return groupName;
    }

    /**
     * Returns a full error code that includes the error's group and code, which uniquely identifies the problem within the group. This is a
     * combination of two most-significant bytes for the error group and two least-significant bytes for the error code.
     *
     * @return Full error code.
     */
    @Override
    public int code() {
        return code;
    }

    /**
     * Returns a human-readable string that represents a full error code. The string format is 'IGN-XXX-nnn', where 'XXX' is the group name
     * and 'nnn' is the unique error code within the group.
     *
     * @return Full error code in a human-readable format.
     */
    public String codeAsString() {
        return ERR_PREFIX + groupName() + '-' + errorCode();
    }

    /**
     * Returns an error group.
     *
     * @see #code()
     * @return Error group.
     */
    @Override
    public short groupCode() {
        return extractGroupCode(code);
    }

    /**
     * Returns an error code that uniquely identifies the problem within a group.
     *
     * @see #code()
     * @see #groupCode()
     * @return Error code.
     */
    @Override
    public short errorCode() {
        return extractErrorCode(code);
    }

    /**
     * Returns a unique identifier of the exception.
     *
     * @return Unique identifier of the exception.
     */
    @Override
    public UUID traceId() {
        return traceId;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return getClass().getName() + ": " + errorMessage(traceId, groupName, code, getLocalizedMessage());
    }
}
