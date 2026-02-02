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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.TraceableException;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction abort exception information stored in {@link TxStateMeta}.
 *
 * <p>This information is stored for cases when a transaction is aborted exceptionally and the original response to the user
 * is lost (for example, due to disconnections). The stored information can later be used to understand the abort reason.
 */
public class TxStateMetaExceptionInfo {
    /** Exception class name. */
    private final String exceptionClassName;

    /** Full Ignite error code (group + code), see {@link TraceableException#code()}. */
    private final int code;

    /** Trace id of the exception. If exception is not traceable will be null. */
    private final @Nullable UUID traceId;

    /** Exception message. */
    private final @Nullable String message;

    /** Original exception object. */
    private final @Nullable Throwable throwable;

    /**
     * Constructor.
     *
     * @param exceptionClassName Exception class name.
     * @param code Full error code (group + code).
     * @param traceId Trace id.
     * @param message Exception message.
     */
    public TxStateMetaExceptionInfo(String exceptionClassName, int code, @Nullable UUID traceId, @Nullable String message) {
        this.exceptionClassName = exceptionClassName;
        this.code = code;
        this.traceId = traceId;
        this.message = message;
        this.throwable = null;
    }

    private TxStateMetaExceptionInfo(
            String exceptionClassName,
            int code,
            @Nullable UUID traceId,
            @Nullable String message,
            @Nullable Throwable throwable
    ) {
        this.exceptionClassName = exceptionClassName;
        this.code = code;
        this.traceId = traceId;
        this.message = message;
        this.throwable = throwable;
    }

    /**
     * Creates an instance from a throwable.
     *
     * <p>If the throwable implements {@link TraceableException}, its {@link TraceableException#code() code} and
     * {@link TraceableException#traceId() traceId} are used. Otherwise {@link INTERNAL_ERR} and {@link #traceId()} produced from
     * the throwable are used.
     *
     * @param throwable Throwable.
     * @return Exception info.
     */
    public static TxStateMetaExceptionInfo fromThrowable(Throwable throwable) {
        Throwable unwrapped = ExceptionUtils.unwrapRootCause(ExceptionUtils.unwrapCause(throwable));

        if (unwrapped instanceof TraceableException) {
            TraceableException traceable = (TraceableException) unwrapped;

            return new TxStateMetaExceptionInfo(
                    unwrapped.getClass().getName(),
                    traceable.code(),
                    traceable.traceId(),
                    unwrapped.getMessage(),
                    unwrapped
            );
        }

        return new TxStateMetaExceptionInfo(
                unwrapped.getClass().getName(),
                INTERNAL_ERR,
                null,
                unwrapped.getMessage(),
                unwrapped
        );
    }

    public String exceptionClassName() {
        return exceptionClassName;
    }

    public int code() {
        return code;
    }

    public @Nullable UUID traceId() {
        return traceId;
    }

    public @Nullable String message() {
        return message;
    }

    public @Nullable Throwable throwable() {
        return throwable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TxStateMetaExceptionInfo that = (TxStateMetaExceptionInfo) o;

        return code == that.code
                && exceptionClassName.equals(that.exceptionClassName)
                && Objects.equals(traceId, that.traceId)
                && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exceptionClassName, code, traceId, message);
    }

    @Override
    public String toString() {
        return "TxStateMetaExceptionInfo["
                + "exceptionClassName='" + exceptionClassName + '\''
                + ", code=" + code
                + ", traceId=" + traceId
                + ", message='" + message + '\''
                + ']';
    }
}
