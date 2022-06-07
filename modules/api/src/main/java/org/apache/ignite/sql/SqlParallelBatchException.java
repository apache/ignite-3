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

/**
 * The subclass of {@link SqlBatchException} thrown when an error occurs during a batch update operation in parallel mode
 * ({@link SessionProperties#BATCH_PARALLEL}).
 */
public class SqlParallelBatchException extends SqlBatchException {
    private final Throwable[] causes;

    /**
     * Creates a new grid exception with the given throwable as a cause and source of error message.
     *
     * @param updCntrs Array that describes the outcome of a batch execution.
     * @param causes Array of errors causes.
     */
    public SqlParallelBatchException(long[] updCntrs, Throwable[] causes) {
        super(updCntrs);

        this.causes = causes;
    }

    /**
     * Returns the array that contains batch execution exceptions.
     *
     * @return Array that contains batch execution exceptions.
     */
    public Throwable[] causes() {
        return causes;
    }
}
