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
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;

/**
 * Versioned Value flavor that allows to be completed explicitly either via a value, or an exception.
 */
public class CompletableVersionedValue<T> implements VersionedValue<T> {
    private final BaseVersionedValue<T> versionedValue;

    /**
     * Constructor with a default history size and no default value.
     */
    public CompletableVersionedValue() {
        this.versionedValue = new BaseVersionedValue<>(null);
    }

    public CompletableVersionedValue(int maxHistorySize) {
        this.versionedValue = new BaseVersionedValue<>(maxHistorySize, null);
    }

    public CompletableVersionedValue(Supplier<T> defaultValueSupplier) {
        this.versionedValue = new BaseVersionedValue<>(defaultValueSupplier);
    }

    @Override
    public CompletableFuture<T> get(long causalityToken) {
        return versionedValue.get(causalityToken);
    }

    @Override
    public @Nullable T latest() {
        return versionedValue.latest();
    }

    @Override
    public long latestCausalityToken() {
        return versionedValue.latestCausalityToken();
    }

    @Override
    public void whenComplete(CompletionListener<T> action) {
        versionedValue.whenComplete(action);
    }

    @Override
    public void removeWhenComplete(CompletionListener<T> action) {
        versionedValue.removeWhenComplete(action);
    }

    /**
     * Completes the Versioned Value for the given token. This method will look for the previous complete token and complete all registered
     * futures in the {@code (prevToken, causalityToken]} range. If no {@code complete} methods have been called before, all these futures
     * will be complete with the configured default value.
     *
     * <p>Calling this method will trigger the {@link #whenComplete} listeners for the given token.
     *
     * @param causalityToken Causality token.
     */
    public void complete(long causalityToken) {
        versionedValue.complete(causalityToken);
    }

    /**
     * Save the version of the value associated with the given causality token. If someone has got a future to await the value associated
     * with the given causality token (see {@link #get(long)}, then the future will be completed.
     *
     * <p>Calling this method will trigger the {@link #whenComplete} listeners for the given token.
     *
     * @param causalityToken Causality token.
     * @param value Current value.
     */
    public void complete(long causalityToken, T value) {
        versionedValue.complete(causalityToken, CompletableFuture.completedFuture(value));
    }

    /**
     * Save the exception associated with the given causality token. If someone has got a future to await the value associated with the
     * given causality token (see {@link #get(long)}, then the future will be completed.
     *
     * <p>Calling this method will trigger the {@link #whenComplete} listeners for the given token.
     *
     * @param causalityToken Causality token.
     * @param throwable An exception.
     */
    public void completeExceptionally(long causalityToken, Throwable throwable) {
        versionedValue.complete(causalityToken, CompletableFuture.failedFuture(throwable));
    }
}
