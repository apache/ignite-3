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

package org.apache.ignite.internal.future;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maintains a collection of in-flight {@link CompletableFuture}s (i.e. futures that are not yet completed) to later
 * have a possibility to process them somehow (for example, cancel them all if we know for sure that they will
 * never be completed).
 */
public class InFlightFutures implements Iterable<CompletableFuture<?>> {
    private final Set<CompletableFuture<?>> inFlightFutures = ConcurrentHashMap.newKeySet();

    /**
     * Registers a future in the in-flight futures collection. When it completes (either normally or exceptionally),
     * it will be removed from the collection.
     *
     * @param future the future to register
     */
    public <T> CompletableFuture<T> registerFuture(CompletableFuture<T> future) {
        inFlightFutures.add(future);

        future.whenComplete((result, ex) -> inFlightFutures.remove(future));
        return future;
    }

    /**
     * Cancels all in-flight futures (that is, the futures that are not yet completed).
     */
    public void cancelInFlightFutures() {
        for (CompletableFuture<?> future : this) {
            future.cancel(true);
        }
    }

    /**
     * Fails all in-flight futures (that is, the futures that are not yet completed).
     *
     * @param cause Exception with which to fail the inflight futures.
     */
    public void failInFlightFutures(Exception cause) {
        for (CompletableFuture<?> future : this) {
            future.completeExceptionally(cause);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<CompletableFuture<?>> iterator() {
        return inFlightFutures.iterator();
    }
}
