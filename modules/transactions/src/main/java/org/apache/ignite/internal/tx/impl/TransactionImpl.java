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

package org.apache.ignite.internal.tx.impl;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;

public class TransactionImpl implements InternalTransaction {
    private final Timestamp timestamp;

    private final TxManager txManager;

    public TransactionImpl(TxManager txManager, Timestamp timestamp) {
        this.txManager = txManager;
        this.timestamp = timestamp;
    }

    @Override public Timestamp timestamp() {
        return timestamp;
    }

    @Override public void commit() {
        // txManager.commit(timestamp);
    }

    @Override public CompletableFuture<Void> commitAsync() {
        return null;
    }

    @Override public void rollback() {

    }

    @Override public CompletableFuture<Void> rollbackAsync() {
        return null;
    }
}
