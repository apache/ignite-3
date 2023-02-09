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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Future utils.
 */
public class ClientFutureUtils {
    public static <T> CompletableFuture<T> doWithRetryAsync(
            Function<RetryContext, CompletableFuture<T>> func,
            Predicate<T> resultValidator) {
        CompletableFuture<T> resFut = new CompletableFuture<>();
        var ctx = new RetryContext();

        apply(func, resultValidator, resFut, ctx);

        return resFut;
    }

    private static <T> void apply(
            Function<RetryContext, CompletableFuture<T>> func,
            Predicate<T> validator,
            CompletableFuture<T> resFut,
            RetryContext ctx) {
        func.apply(ctx).whenComplete((res, err) -> {
            if (err == null && validator.test(res)) {
                resFut.complete(res);
                return;
            }

            ctx.attempt++;

            if (err != null) {
                ctx.errors.add(err);
            }

            apply(func, validator, resFut, ctx);
        });
    }

    public static class RetryContext {
        public int attempt;
        public ArrayList<Throwable> errors = new ArrayList<>();
    }
}
