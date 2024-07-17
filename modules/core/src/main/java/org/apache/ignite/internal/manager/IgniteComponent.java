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

package org.apache.ignite.internal.manager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import org.jetbrains.annotations.TestOnly;

/**
 * Common interface for ignite components that provides entry points for component lifecycle flow.
 */
public interface IgniteComponent {
    /**
     * Starts the component. Depending on component flow both configuration properties listeners, meta storage watch registration, starting
     * thread pools and threads goes here.
     *
     * <p>All actions in the component startup is divided into two categories: sync actions,
     * that can be executed synchronously in order for the component to be usable by other components during their startup, and async
     * actions, that are wrapped in a CompletableFuture and returned from the start method.
     *
     * @param componentContext The component lifecycle context.
     * @return Future that will be completed when the asynchronous part of the start is processed.
     */
    CompletableFuture<Void> startAsync(ComponentContext componentContext);

    /**
     * Triggers running before node stop logic. It's guaranteed that during beforeNodeStop all components beneath given one are still
     * running and node is a part of topology.
     */
    default void beforeNodeStop() {
        // No-op.
    }

    /**
     * Stops the component. It's guaranteed that during {@code IgniteComponent#stopAsync(ExecutorService))} all components beneath given one
     * are still running, however the node is no longer part of the topology and, accordingly, network interaction is impossible.
     *
     * @param componentContext The component lifecycle context.
     * @return Future that will be completed when the asynchronous part of the stop is processed.
     */
    CompletableFuture<Void> stopAsync(ComponentContext componentContext);


    /**
     * Stops the component. Calls {@link IgniteComponent#stopAsync(ComponentContext)} with {@link ForkJoinPool#commonPool()}.
     *
     * @return Future that will be completed when the asynchronous part of the stop is processed.
     */
    @TestOnly
    default CompletableFuture<Void> stopAsync() {
        return stopAsync(new ComponentContext());
    }
}
