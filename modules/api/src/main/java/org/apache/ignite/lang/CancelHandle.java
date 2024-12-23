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

package org.apache.ignite.lang;

import java.util.concurrent.CompletableFuture;

/**
 * A handle which may be used to request the cancellation of execution.
 */
public interface CancelHandle {

    /** A factory method to create a handle. */
    static CancelHandle create() {
        return new CancelHandleImpl();
    }

    /**
     * Abruptly terminates an execution of an associated process.
     *
     * <p>Control flow will return after the process has been terminated and the resources associated with that process have been freed.
     */
    void cancel();

    /**
     * Abruptly terminates an execution of a associated process.
     *
     * @return A future that will be completed after the process has been terminated and the resources associated with that process have
     *         been freed.
     */
    CompletableFuture<Void> cancelAsync();

    /**
     * Flag indicating whether cancellation was requested or not.
     *
     * <p>This method will return true even if cancellation has not been completed yet.
     *
     * @return {@code true} when cancellation was requested.
     */
    boolean isCancelled();

    /**
     * Issue a token associated with this handle.
     *
     * <p>Token is reusable, meaning the same token may be used to link several executions into a single cancellable.
     */
    CancellationToken token();
}
