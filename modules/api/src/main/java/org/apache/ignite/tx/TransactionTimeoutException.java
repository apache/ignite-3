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

package org.apache.ignite.tx;

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR;

/**
 * Thrown when a transaction exceeds its timeout.
 */
public class TransactionTimeoutException extends TransactionException {
    private static final String DEFAULT_MESSAGE = "Transaction timeout exceeded.";

    /**
     * Creates a new exception with the default timeout error code and message.
     */
    public TransactionTimeoutException() {
        super(TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR, DEFAULT_MESSAGE);
    }

}
