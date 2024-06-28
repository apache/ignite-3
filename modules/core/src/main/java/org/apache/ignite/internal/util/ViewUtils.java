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

package org.apache.ignite.internal.util;

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;

/**
 * Views utilities.
 */
public final class ViewUtils {
    /**
     * Wraps an exception in an IgniteException, extracting trace identifier and error code when the specified exception or one of its
     * causes is an IgniteException itself.
     *
     * @param e Internal exception.
     * @return Public exception.
     */
    public static Throwable ensurePublicException(Throwable e) {
        Objects.requireNonNull(e);

        e = ExceptionUtils.unwrapCause(e);

        if (e instanceof IgniteException) {
            return copyExceptionWithCauseIfPossible((IgniteException) e);
        }

        if (e instanceof IgniteCheckedException) {
            return copyExceptionWithCauseIfPossible((IgniteCheckedException) e);
        }

        e = IgniteExceptionMapperUtil.mapToPublicException(e);

        return new IgniteException(INTERNAL_ERR, e.getMessage(), e);
    }

    /**
     * Try to copy exception using ExceptionUtils.copyExceptionWithCause and return new exception if it was not possible.
     *
     * @param e Exception.
     * @return Properly copied exception or a new error, if exception can not be copied.
     */
    private static <T extends Throwable & TraceableException> Throwable copyExceptionWithCauseIfPossible(T e) {
        Throwable copy = ExceptionUtils.copyExceptionWithCause(e.getClass(), e.traceId(), e.code(), e.getMessage(), e);
        if (copy != null) {
            return copy;
        }

        return new IgniteException(INTERNAL_ERR, "Public Ignite exception-derived class does not have required constructor: "
                + e.getClass().getName(), e);
    }

    /**
     * Waits for async operation completion.
     *
     * @param future Future to wait to.
     * @param <T> Future result type.
     * @return Future result.
     */
    public static <T> T sync(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag.

            throw ExceptionUtils.sneakyThrow(ensurePublicException(e));
        } catch (ExecutionException e) {
            throw ExceptionUtils.sneakyThrow(ensurePublicException(e));
        }
    }

    /**
     * Checks given keys collection that it isn't a null-value and it doesn't contain null-values inside.
     *
     * @param keys Collection of keys to check.
     * @param <K> Keys' type
     * @throws NullPointerException in case if keys collection is null or any key is null.
     */
    public static <K> void checkKeysForNulls(Collection<K> keys) {
        Objects.requireNonNull(keys, "keys");

        keys.forEach(key -> Objects.requireNonNull(key, "key"));
    }

    private ViewUtils() { }
}
