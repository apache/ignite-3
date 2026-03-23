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
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.StripedVersatileReadWriteLock;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;

/**
 * A lock that allows to linearize user operations (which are executed in an attached state against current {@link Ignite} instance)
 * wrt restarts (which are executed in a detached state).
 */
public class IgniteAttachmentLock {
    /** This must always be accessed under {@link #lock}. */
    private final Supplier<Ignite> igniteRef;

    private final StripedVersatileReadWriteLock lock;

    /**
     * Constructor.
     */
    public IgniteAttachmentLock(Supplier<Ignite> igniteRef, Executor asyncContinuationExecutor) {
        this.igniteRef = igniteRef;
        lock = new StripedVersatileReadWriteLock(asyncContinuationExecutor);
    }

    /**
     * Executes the given synchronous action in an attached state (that is, without interference with a restart).
     *
     * @param action Action to execute.
     * @return Action result.
     */
    public <T> T attached(Function<? super Ignite, ? extends T> action) {
        // TODO: IGNITE-23009 - add a timeout.
        lock.readLock();

        try {
            return action.apply(actualIgniteOrThrow());
        } finally {
            lock.readUnlock();
        }
    }

    /**
     * Executes the given synchronous action in an attached state (that is, without interference with a restart).
     *
     * @param action Action to execute.
     */
    public void consumeAttached(Consumer<? super Ignite> action) {
        // TODO: IGNITE-23009 - add a timeout.
        lock.readLock();

        try {
            action.accept(actualIgniteOrThrow());
        } finally {
            lock.readUnlock();
        }
    }

    /**
     * Executes the given asynchronous action in an attached state (that is, without interference with a restart). Ignite remains attached
     * until the action future completes.
     *
     * @param action Action to execute.
     * @return Action future.
     */
    public <T> CompletableFuture<T> attachedAsync(Function<? super Ignite, ? extends CompletableFuture<T>> action) {
        // TODO: IGNITE-23009 - add a timeout.
        return lock.inReadLockAsync(() -> action.apply(actualIgniteOrThrow()));
    }

    private Ignite actualIgniteOrThrow() {
        Ignite ignite = igniteRef.get();

        if (ignite == null) {
            // TODO: IGNITE-23020 - throw a specific exception with a specific error code.
            throw new IgniteException(Common.INTERNAL_ERR, "The node is already shut down.");
        }

        return ignite;
    }

    /**
     * Executes the given asynchronous action in a detached state (that is, without interference with user operations).
     *
     * @param action Action to execute.
     * @return Action future.
     */
    public <T> CompletableFuture<T> detachedAsync(Supplier<? extends CompletableFuture<T>> action) {
        return lock.inWriteLockAsync(action);
    }
}
