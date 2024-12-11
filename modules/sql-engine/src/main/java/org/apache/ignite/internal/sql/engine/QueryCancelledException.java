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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;

import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * The exception is thrown if a query was cancelled or timed out while executing.
 */
public class QueryCancelledException extends IgniteException {
    private static final long serialVersionUID = 0L;

    public static final String CANCEL_MSG = "The query was cancelled while executing.";

    public static final String TIMEOUT_MSG = "Query timeout";

    /**
     * Default constructor.
     */
    public QueryCancelledException() {
        super(EXECUTION_CANCELLED_ERR, CANCEL_MSG);
    }

    /**
     * Constructor.
     *
     * @param message Error message.
     */
    public QueryCancelledException(String message) {
        super(EXECUTION_CANCELLED_ERR, message);
    }

    /**
     * Constructor.
     *
     * @param cause Cause.
     */
    public QueryCancelledException(@Nullable Throwable cause) {
        super(EXECUTION_CANCELLED_ERR, CANCEL_MSG, cause);
    }
}
