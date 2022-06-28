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

import static org.apache.ignite.internal.err.ErrorGroup.UNKNOWN_ERR;
import static org.apache.ignite.internal.err.ErrorGroup.UNKNOWN_ERR_GROUP;

import java.util.UUID;
import org.apache.ignite.internal.err.ErrorGroup;
import org.jetbrains.annotations.Nullable;

/**
 * General internal exception. This exception is used to indicate any error condition within the node.
 */
public class IgniteInternalException extends RuntimeException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Error group. */
    private final ErrorGroup group;

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
     * Creates a new exception with the given group and error code.
     *
     * @param traceId Unique identifier of this exception.
     * @param group Error group.
     * @param code Full error code.
     */
    public IgniteInternalException(UUID traceId, ErrorGroup group, int code) {
        this.traceId = traceId;
        this.group = group;
        this.code = code;
    }

    /**
     * Creates a new exception with the given group, error code and detail message.
     *
     * @param traceId Unique identifier of this exception.
     * @param group Error group.
     * @param code Full error code.
     * @param message Detail message.
     */
    public IgniteInternalException(UUID traceId, ErrorGroup group, int code, String message) {
        super(message);

        this.traceId = traceId;
        this.group = group;
        this.code = code;
    }

    /**
     * Creates a new exception with the given group, error code and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param group Error group.
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteInternalException(UUID traceId, ErrorGroup group, int code, Throwable cause) {
        super(cause);

        this.traceId = traceId;
        this.group = group;
        this.code = code;
    }

    /**
     * Creates a new exception with the given group, error code, detail message and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param group Error group.
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteInternalException(UUID traceId, ErrorGroup group, int code, String message, Throwable cause) {
        super(message, cause);

        this.traceId = traceId;
        this.group = group;
        this.code = code;
    }

    /**
     * Creates an empty exception.
     */
    @Deprecated
    public IgniteInternalException() {
        this.traceId = null;
        this.group = UNKNOWN_ERR_GROUP;
        this.code = UNKNOWN_ERR;
    }

    /**
     * Creates a new exception with the given error message.
     *
     * @param msg Error message.
     */
    @Deprecated
    public IgniteInternalException(String msg) {
        super(msg);

        this.traceId = null;
        this.group = UNKNOWN_ERR_GROUP;
        this.code = UNKNOWN_ERR;
    }

    /**
     * Creates a new grid exception with the given throwable as a cause and source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    @Deprecated
    public IgniteInternalException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates a new exception with the given error message and optional nested exception.
     *
     * @param msg   Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    @Deprecated
    public IgniteInternalException(String msg, @Nullable Throwable cause) {
        super(msg, cause);

        this.traceId = null;
        this.group = UNKNOWN_ERR_GROUP;
        this.code = UNKNOWN_ERR;
    }

    /**
     * Returns a group name of this error.
     *
     * @return Group name.
     */
    public String groupName() {
        return group.name();
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
     * Returns error group.
     *
     * @return Error group.
     */
    public int groupCode() {
        return group.code();
    }

    /**
     * Returns error code that uniquely identifies a problem within a group.
     *
     * @see #code()
     * @see #groupCode()
     * @return Error code.
     */
    public int errorCode() {
        return code() & 0xFFFF;
    }

    /**
     * Returns an unique identifier of this exception.
     *
     * @return Unique identifier of this exception.
     */
    public UUID traceId() {
        return traceId;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "IGN-" + group.name() + '-' + errorCode() + " Trace ID:" + traceId() + ' ' + super.toString();
    }
}
