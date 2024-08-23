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
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.Ignite;

/**
 * Base for references to API objects under a swappable {@link Ignite}.
 */
abstract class RestartProofApiObject<T> {
    private final IgniteAttachmentLock attachmentLock;

    private final RefCache<T> delegateCache;

    RestartProofApiObject(IgniteAttachmentLock attachmentLock, Ignite initialIgnite, Function<Ignite, T> delegateFactory) {
        this.attachmentLock = attachmentLock;

        delegateCache = new RefCache<>(initialIgnite, delegateFactory);
    }

    /**
     * Executes the given synchronous action in an attached state.
     *
     * @param action Action on an object.
     * @return Action result.
     */
    final <U> U attached(Function<T, U> action) {
        return attachmentLock.attached(ignite -> action.apply(actualInstanceFor(ignite)));
    }

    /**
     * Executes the given synchronous action in an attached state.
     *
     * @param action Action on an object.
     */
    final void consumeAttached(Consumer<T> action) {
        attachmentLock.consumeAttached(ignite -> action.accept(actualInstanceFor(ignite)));
    }

    /**
     * Executes the given asynchronous action in an attached state.
     *
     * @param action Action on an object.
     * @return Action future.
     */
    final <U> CompletableFuture<U> attachedAsync(Function<T, CompletableFuture<U>> action) {
        return attachmentLock.attachedAsync(ignite -> action.apply(actualInstanceFor(ignite)));
    }

    private T actualInstanceFor(Ignite ignite) {
        return delegateCache.actualFor(ignite);
    }
}
