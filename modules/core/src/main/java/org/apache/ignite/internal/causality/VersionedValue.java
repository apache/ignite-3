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

/**
 * Versioned Value flavor that allows to be completed explicitly either via a value, or an exception.
 */
public class VersionedValue<T> extends AbstractVersionedValue<T> {
    /**
     * Constructor with a default history size and no default value.
     */
    public VersionedValue() {
        super(null);
    }

    public VersionedValue(int maxHistorySize) {
        super(maxHistorySize, null);
    }

    public VersionedValue(Supplier<T> defaultValueSupplier) {
        super(defaultValueSupplier);
    }

    /**
     * Save the version of the value associated with the given causality token. If someone has got a future to await the value associated
     * with the given causality token (see {@link #get(long)}, then the future will be completed.
     *
     * @param causalityToken Causality token.
     * @param value          Current value.
     */
    public void complete(long causalityToken, T value) {
        complete(causalityToken, CompletableFuture.completedFuture(value));
    }

    /**
     * Save the exception associated with the given causality token. If someone has got a future to await the value associated
     * with the given causality token (see {@link #get(long)}, then the future will be completed.
     *
     * @param causalityToken Causality token.
     * @param throwable An exception.
     */
    public void completeExceptionally(long causalityToken, Throwable throwable) {
        complete(causalityToken, CompletableFuture.failedFuture(throwable));
    }
}
