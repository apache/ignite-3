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

package org.apache.ignite.internal.causality;

import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Parametrized type to store several versions of a value.
 *
 * <p>The value can be available through the causality token, which is represented by a {@code long}.
 *
 * @param <T> Type of real value.
 */
public interface VersionedValue<T> {
    /**
     * Creates a future for this value and causality token, or returns it if it already exists.
     *
     * <p>The returned future is associated with an update having the given causality token and completes when this update is finished.
     *
     * @param causalityToken Causality token. Let's assume that the update associated with token N is already applied to this value.
     *         Then, if token N is given as an argument, a completed future will be returned. If token N - 1 is given, this method returns
     *         the result in the state that is actual for the given token. If the token is strongly outdated, {@link OutdatedTokenException}
     *         is thrown. If token N + 1 is given, this method will return a future that will be completed when the update associated with
     *         token N + 1 will have been applied. Tokens that greater than N by more than 1 should never be passed.
     * @return The future.
     * @throws OutdatedTokenException If outdated token is passed as an argument.
     */
    CompletableFuture<T> get(long causalityToken);

    /**
     * Gets the latest value of completed future.
     */
    @Nullable T latest();

    /**
     * Returns the latest completed causality token. Negative value if not initialized.
     */
    long latestCausalityToken();

    /**
     * Add listener for completions of this versioned value on every token.
     *
     * @param action Action to perform.
     */
    void whenComplete(CompletionListener<T> action);

    /**
     * Removes a completion listener, see {@link #whenComplete}.
     *
     * @param action Action to remove.
     */
    void removeWhenComplete(CompletionListener<T> action);
}
