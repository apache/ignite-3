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

package org.apache.ignite.internal.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;

/**
 * Future utils.
 */
class ClientFutureUtils {
    static <T> @Nullable T getNowSafe(CompletableFuture<T> fut) {
        try {
            return fut.getNow(null);
        } catch (Throwable e) {
            return null;
        }
    }

    static <T> CompletableFuture<T> doWithRetryAsync(
            Supplier<CompletableFuture<T>> func,
            Predicate<RetryContext> retryPredicate) {
        CompletableFuture<T> resFut = new CompletableFuture<>();
        RetryContext ctx = new RetryContext();

        doWithRetryAsync(func, retryPredicate, resFut, ctx);

        return resFut;
    }

    private static <T> void doWithRetryAsync(
            Supplier<CompletableFuture<T>> func,
            Predicate<RetryContext> retryPredicate,
            CompletableFuture<T> resFut,
            RetryContext ctx) {
        func.get().whenComplete((res, err) -> {
            try {
                if (err == null) {
                    resFut.complete(res);
                    return;
                }

                Throwable resErr = null;

                // This code is executed by different threads, but not concurrently.
                // Use synchronized block to modify ctx for simplicity (instead of volatile).
                synchronized (ctx) {
                    if (ctx.errors == null) {
                        ctx.errors = new ArrayList<>();
                    }

                    ctx.errors.add(err);

                    if (retryPredicate.test(ctx)) {
                        ctx.attempt++;
                    } else {
                        resErr = ctx.errors.get(0);

                        HashSet<Throwable> dejaVu = new HashSet<>();
                        existingCauseOrSuppressed(resErr, dejaVu); // Seed dejaVu.

                        for (int i = 1; i < ctx.errors.size(); i++) {
                            Throwable e = ctx.errors.get(i);

                            if (!existingCauseOrSuppressed(e, dejaVu)) {
                                resErr.addSuppressed(e);
                            }
                        }
                    }
                }

                if (resErr != null) {
                    resFut.completeExceptionally(resErr);
                } else {
                    doWithRetryAsync(func, retryPredicate, resFut, ctx);
                }
            } catch (Throwable t) {
                resFut.completeExceptionally(t);
            }
        });
    }

    private static boolean existingCauseOrSuppressed(Throwable t, HashSet<Throwable> dejaVu) {
        if (t == null) {
            return false;
        }

        if (!dejaVu.add(t)) {
            return true;
        }

        for (Throwable sup : t.getSuppressed()) {
            if (existingCauseOrSuppressed(sup, dejaVu)) {
                return true;
            }
        }

        return existingCauseOrSuppressed(t.getCause(), dejaVu);
    }

    static class RetryContext {
        int attempt;

        @Nullable ArrayList<Throwable> errors;

        @Nullable Throwable lastError() {
            return errors == null || errors.isEmpty()
                    ? null
                    : errors.get(errors.size() - 1);
        }
    }
}
