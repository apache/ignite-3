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

import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
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
 * Table view utilities.
 */
public final class ViewUtils {
    /**
     * Waits for async operation completion.
     *
     * @param fut Future to wait to.
     * @param <T> Future result type.
     * @return Future result.
     */
    public static <T> T sync(CompletableFuture<T> fut) {
        try {
            return fut.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag.

            throw sneakyThrow(ensurePublicException(e));
        } catch (ExecutionException e) {
            Throwable cause = unwrapCause(e);

            throw sneakyThrow(ensurePublicException(cause));
        }
    }

    /**
     * Wraps an exception in an IgniteException, extracting trace identifier and error code when the specified exception or one of its
     * causes is an IgniteException itself.
     *
     * @param e Internal exception.
     * @return Public exception.
     */
    public static Throwable ensurePublicException(Throwable e) {
        Objects.requireNonNull(e);

        e = unwrapCause(e);

        if (e instanceof IgniteException) {
            return copyExceptionWithCauseIfPossible((IgniteException) e);
        }

        if (e instanceof IgniteCheckedException) {
            return copyExceptionWithCauseIfPossible((IgniteCheckedException) e);
        }

        var e0 = IgniteExceptionMapperUtil.mapToPublicException(e);

        return new IgniteException(INTERNAL_ERR, e0.getMessage(), e0);
    }

    /**
     * Try to copy exception using ExceptionUtils.copyExceptionWithCause and return new exception if it was not possible.
     *
     * @param e Exception.
     * @return Properly copied exception or a new error, if exception can not be copied.
     */
    // TODO: consider removing after IGNITE-22721 gets resolved.
    private static <T extends Throwable & TraceableException> Throwable copyExceptionWithCauseIfPossible(T e) {
        Throwable copy = ExceptionUtils.copyExceptionWithCause(e.getClass(), e.traceId(), e.code(), e.getMessage(), e);
        if (copy != null) {
            return copy;
        }

        return new IgniteException(INTERNAL_ERR, "Public Ignite exception-derived class does not have required constructor: "
                + e.getClass().getName(), e);
    }

    /**
     * Checks that given keys collection isn't null and there is no a null-value key.
     *
     * @param keys Given keys collection.
     * @param <K> Keys type.
     * @throws NullPointerException In case if the collection null either any key is null.
     */
    public static <K> void checkKeysForNulls(Collection<K> keys) {
        checkCollectionForNulls(keys, "keys", "key");
    }

    /**
     * Checks that given collection isn't null and there is no a null-value element.
     *
     * @param coll Given collection.
     * @param collectionName Collection name.
     * @param elementName Element name.
     * @param <K> Keys type.
     * @throws NullPointerException In case if the collection null either any key is null.
     */
    public static <K> void checkCollectionForNulls(Collection<K> coll, String collectionName, String elementName) {
        Objects.requireNonNull(coll, collectionName);

        for (K key : coll) {
            Objects.requireNonNull(key, elementName);
        }
    }

    private ViewUtils() { }
}
