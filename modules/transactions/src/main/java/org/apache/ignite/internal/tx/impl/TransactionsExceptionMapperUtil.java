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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.util.CompletableFutures.isCompletedSuccessfully;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.lang.TraceableException;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;

/**
 * Utils for mapping exceptions that is specific to the {@link Transaction} context.
 */
class TransactionsExceptionMapperUtil {
    /**
     * Returns a new CompletableFuture that, when the given {@code origin} future completes exceptionally, maps the origin's exception to a
     * public Ignite exception if it is needed. The mapping is made in the context of {@link Transaction} methods.
     *
     * @param origin The future to use to create a new stage.
     * @param <T> Type os result.
     * @return New CompletableFuture.
     */
    static <T> CompletableFuture<T> convertToPublicFuture(CompletableFuture<T> origin, int defaultCode) {
        if (isCompletedSuccessfully(origin)) {
            // No need to translate exceptions.
            return origin;
        }

        return origin
                .handle((res, err) -> {
                    if (err != null) {
                        throw new CompletionException(mapToPublicTransactionException(unwrapCause(err), defaultCode));
                    }

                    return res;
                });
    }

    private static Throwable mapToPublicTransactionException(Throwable origin, int defaultCode) {
        if (origin instanceof PrimaryReplicaExpiredException) {
            PrimaryReplicaExpiredException err = (PrimaryReplicaExpiredException) origin;

            return new TransactionException(err.traceId(), err.code(), err.getMessage(), err);
        }
        if (origin instanceof AssertionError) {
            return new TransactionException(defaultCode, origin);
        }
        if (origin instanceof Error) {
            return origin;
        }

        Throwable mapped = mapToPublicException(origin, ex -> new TransactionException(defaultCode, ex));

        if (mapped instanceof TransactionException) {
            return mapped;
        }

        if (mapped instanceof TraceableException) {
            TraceableException traceable = (TraceableException) mapped;

            return new TransactionException(traceable.traceId(), traceable.code(), mapped.getMessage(), mapped);
        }

        return new TransactionException(defaultCode, mapped);
    }
}
