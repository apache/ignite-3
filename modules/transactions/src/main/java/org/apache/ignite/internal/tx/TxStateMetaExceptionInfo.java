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

import java.io.Serializable;
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
public class TxStateMetaExceptionInfo implements Serializable {
    private static final long serialVersionUID = 4057922085423696869L;

    /** Exception class name. */
    private final String exceptionClassName;

    /** Full Ignite error code (group + code), see {@link TraceableException#code()}. */
    private final int code;

    /** Trace id of the exception. If exception is not traceable will be null. */
    private final @Nullable UUID traceId;

    /** Exception message (may be {@code null}). */
    private final @Nullable String message;

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
                    unwrapped.getMessage()
            );
        }

        return new TxStateMetaExceptionInfo(
                unwrapped.getClass().getName(),
                INTERNAL_ERR,
                null,
                unwrapped.getMessage()
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TxStateMetaExceptionInfo that = (TxStateMetaExceptionInfo) o;

        if (code != that.code) {
            return false;
        }
        if (!exceptionClassName.equals(that.exceptionClassName)) {
            return false;
        }
        if (!traceId.equals(that.traceId)) {
            return false;
        }
        return message != null ? message.equals(that.message) : that.message == null;
    }

    @Override
    public int hashCode() {
        int result = exceptionClassName.hashCode();
        result = 31 * result + Integer.hashCode(code);
        result = 31 * result + traceId.hashCode();
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TxStateMetaExceptionInfo{"
                + "exceptionClassName='" + exceptionClassName + '\''
                + ", code=" + code
                + ", traceId=" + traceId
                + ", message='" + message + '\''
                + '}';
    }
}
