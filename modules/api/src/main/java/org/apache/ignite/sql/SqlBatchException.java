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

package org.apache.ignite.sql;

import org.apache.ignite.internal.util.ArrayUtils;

/**
 * The subclass of {@link SqlException} thrown when an error occurs during a batch update operation. In addition to the
 * information provided by {@link SqlException}, a <code>SqlBatchException</code> provides the update
 * counts for all commands that were executed successfully during the batch update, that is,
 * all commands that were executed before the error occurred. The order of elements in an array of update counts
 * corresponds to the order in which commands were added to the batch.
 *
 */
public class SqlBatchException extends SqlException {
    private final long[] updCntrs;

    /**
     * Creates a new grid exception with the given throwable as a cause and source of error message.
     *
     * @param updCntrs Array that describes the outcome of a batch execution.
     * @param cause Non-null throwable cause.
     */
    public SqlBatchException(long[] updCntrs, Throwable cause) {
        super(cause);

        this.updCntrs = updCntrs != null ? updCntrs : ArrayUtils.LONG_EMPTY_ARRAY;
    }

    /**
     * Returns the array that describes the outcome of a batch execution.
     *
     * @return Array that describes the outcome of a batch execution.
     */
    public long[] updateCounters() {
        return updCntrs;
    }
}
