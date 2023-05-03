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

package org.apache.ignite.internal.cli.core.repl.registry.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/** Lazy async reference that will fetch the value from the source until the value is not null. */
public final class LazyObjectRef<R> {

    private static final IgniteLogger LOG = CliLoggers.forClass(LazyObjectRef.class);

    private final Supplier<R> source;

    private final AtomicReference<R> ref = new AtomicReference<>(null);

    private final AtomicReference<Boolean> execInProgress = new AtomicReference<>(false);

    public LazyObjectRef(Supplier<R> source) {
        this.source = source;
        fetchFrom(source);
    }

    private void fetchFrom(Supplier<R> source) {
        execInProgress.set(true);

        CompletableFuture.supplyAsync(source)
                .thenAccept(ref::set)
                .whenComplete((v, t) -> {
                    if (t != null) {
                        LOG.warn("Got exception when fetch from source", t);
                    }
                    execInProgress.set(false);
                });
    }

    /** Returns {@code null} if the fetching is in progress or the value returned from the source is {@code null}. */
    @Nullable
    public R get() {
        if (ref.get() == null && execInProgress.get()) {
            return null;
        }

        if (ref.get() == null && !execInProgress.get()) {
            fetchFrom(source);
        }

        return ref.get();
    }
}
