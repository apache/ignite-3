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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Subclass of {@link SqlException} is thrown when an error occurs during a batch update operation. In addition to the
 * information provided by {@link SqlException}, <code>SqlBatchException</code> provides the update
 * counts for all commands that were executed successfully during the batch update, that is,
 * all commands that were executed before the error occurred. The order of elements in the array of update counts
 * corresponds to the order in which these commands were added to the batch.
 *
 */
public class SqlBatchException extends SqlException {
    /** Empty array of long. */
    private static final long[] LONG_EMPTY_ARRAY = new long[0];

    private final long[] updCntrs;

    /**
     * Creates a grid exception with the given throwable as a cause and source of error message.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param updCntrs Array that describes the outcome of a batch execution.
     * @param cause Non-null throwable cause.
     */
    public SqlBatchException(UUID traceId, int code, long[] updCntrs, Throwable cause) {
        super(traceId, code, cause.getMessage(), cause);

        this.updCntrs = updCntrs != null ? updCntrs : LONG_EMPTY_ARRAY;
    }

    /**
     * Creates an exception with the given error message.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param updCntrs Array that describes the outcome of a batch execution.
     * @param message Detailed message.
     */
    public SqlBatchException(UUID traceId, int code, long[] updCntrs, String message) {
        super(traceId, code, message, null);

        this.updCntrs = updCntrs != null ? updCntrs : LONG_EMPTY_ARRAY;
    }

    /**
     * Creates an exception with the given trace ID, error code, detailed message, and cause.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param message Detailed message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public SqlBatchException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);

        while ((cause instanceof CompletionException || cause instanceof ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }

        updCntrs = cause instanceof SqlBatchException ? ((SqlBatchException) cause).updCntrs : null;
    }

    /**
     * Returns an array that describes the outcome of a batch execution.
     *
     * @return Array that describes the outcome of a batch execution.
     */
    public long[] updateCounters() {
        return updCntrs;
    }
}
