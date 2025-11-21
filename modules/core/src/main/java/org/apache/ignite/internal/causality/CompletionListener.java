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
 * Listener that will be notified of every completion of a Versioned Value.
 *
 * @see VersionedValue#whenComplete(CompletionListener)
 */
@FunctionalInterface
public interface CompletionListener<T> {
    /**
     * Method that will be called on every completion of a Versioned Value.
     *
     * @param token Token for which a value has been completed.
     * @param value Value that the Versioned Value was completed with.
     * @param ex If not {@code null} - the Versioned Value has benn completed with an exception.
     * @return Future that signifies the end of the event execution.
     */
    CompletableFuture<?> whenComplete(long token, @Nullable T value, @Nullable Throwable ex);
}
