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

package org.apache.ignite.internal.restart;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Reference to {@link IgniteTransactions} under a swappable {@link Ignite} instance. When a restart happens, this switches to
 * the new Ignite instance.
 *
 * <p>API operations on this are linearized wrt node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
class RestartProofIgniteTransactions implements IgniteTransactions, Wrapper {
    private final IgniteAttachmentLock attachmentLock;

    RestartProofIgniteTransactions(IgniteAttachmentLock attachmentLock) {
        this.attachmentLock = attachmentLock;
    }

    @Override
    public Transaction begin(@Nullable TransactionOptions options) {
        // TODO: IGNITE-23061 - wrap in a restart-proof implementation of Transaction.
        return attachmentLock.attached(ignite -> ignite.transactions().begin(options));
    }

    @Override
    public CompletableFuture<Transaction> beginAsync(@Nullable TransactionOptions options) {
        // TODO: IGNITE-23061 - wrap in a restart-proof implementation of Transaction.
        return attachmentLock.attachedAsync(ignite -> ignite.transactions().beginAsync(options));
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attachmentLock.attached(ignite -> Wrappers.unwrap(ignite.transactions(), classToUnwrap));
    }
}
