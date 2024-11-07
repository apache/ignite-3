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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.CancelHandleHelper;
import org.apache.ignite.lang.CancellationToken;

/** Cancellation token for SQL operations. */
public class SqlCancellationToken {

    private final CancellationToken cancellationToken;

    /** Constructor. */
    public SqlCancellationToken(CancellationToken cancellationToken) {
        this.cancellationToken = Objects.requireNonNull(cancellationToken, "cancellationToken");
    }

    /**
     * Add the given cancellable operation to this token.
     *
     * @see CancelHandleHelper#addCancelAction(CancellationToken, Runnable, CompletableFuture)
     */
    public void addOperation(Runnable cancelAction, CompletableFuture<?> cancelFut) {
        CancelHandleHelper.addCancelAction(cancellationToken, cancelAction, cancelFut);
    }

    /**
     * Returns {@code true} if cancellation was requested.
     *
     * @see CancelHandleHelper#isCancelled(CancellationToken)
     */
    public boolean isCancelled() {
        return CancelHandleHelper.isCancelled(cancellationToken);
    }
}
