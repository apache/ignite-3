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

package org.apache.ignite.internal.partition.replicator.raft;

import java.io.Serializable;
import org.apache.ignite.internal.tx.TransactionResult;

/**
 * Result of the finish transaction command processing.
 */
public class FinishTxCommandResult implements Serializable {
    private static final long serialVersionUID = 6975138213202466038L;

    private final boolean applied;
    private final TransactionResult transactionResult;

    public FinishTxCommandResult(boolean applied, TransactionResult transactionResult) {
        this.applied = applied;
        this.transactionResult = transactionResult;
    }

    /**
     * Returns {@code true} if the finish command was applied, {@code false} if it was skipped.
     */
    public boolean wasApplied() {
        return applied;
    }

    /**
     * Returns the transaction result from the state storage.
     */
    public TransactionResult transactionResult() {
        return transactionResult;
    }
}

