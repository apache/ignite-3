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

package org.apache.ignite.internal.event;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** A listener that handles events from an event producer. */
@FunctionalInterface
public interface EventListener<P extends EventParameters> {
    /**
     * Notifies the listener about an event.
     *
     * @param parameters Properties of the event.
     * @return Completable future, which is completed when event handling is finished. The {@code true} value of the future means that the
     *         event has been handled and a listener will be removed, {@code false} is that the listener will continue listening. This
     *         future will never be completed with {@code null} value.
     */
    CompletableFuture<Boolean> notify(P parameters);

    /**
     * Creates an adapter for a given callback.
     *
     * <p>Created listener will never return a future completed with {@code true}.
     */
    static <P extends EventParameters> EventListener<P> fromConsumer(Consumer<P> callback) {
        return parameters -> {
            try {
                callback.accept(parameters);
            } catch (Throwable e) {
                return failedFuture(e);
            }

            return falseCompletedFuture();
        };
    }
}
