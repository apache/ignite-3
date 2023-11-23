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
import org.apache.ignite.lang.ErrorGroups.Sql;

/**
 * The exception is thrown if a query was cancelled or timed out while executing.
 */
public class ExternalTransactionNotSupportedException extends SqlException {
    /**
     * Default constructor.
     */
    public ExternalTransactionNotSupportedException() {
        super(Sql.TX_CONTROL_INSIDE_EXTERNAL_TX_ERR, "Transaction control statement cannot be executed within an external transaction.");
    }

    /**
     * Constructor is required to copy this exception.
     */
    @Deprecated
    public ExternalTransactionNotSupportedException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
