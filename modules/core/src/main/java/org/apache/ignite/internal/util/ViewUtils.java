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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
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
     * Ensures the provided exception complies with our public API.
     * <ol>
     *   <li>Finds the root cause of the exception.</li>
     *   <li>Errors caused by {@link IgniteException} and {@link IgniteCheckedException} are treated as server-side exceptions.
     *      Their stack trace is rebased to the current stack trace.</li>
     *   <li>Other errors are mapped using {@link IgniteExceptionMapperUtil#mapToPublicException(Throwable, Function)},
     *      are treated as client-side errors, and their stack trace is not rebased.</li>
     * </ol>
     *
     * @param e Internal exception.
     * @return Public exception.
     */
    public static Throwable ensurePublicException(Throwable e) {
        Objects.requireNonNull(e);

        // Copy should rebase the stacktrace.
        if (e instanceof IgniteException || e instanceof IgniteCheckedException) {
            return copyExceptionWithCauseIfPossible((Throwable & TraceableException) e);
        }

        return IgniteExceptionMapperUtil.mapToPublicException(e, ex -> new IgniteException(INTERNAL_ERR, ex.getMessage(), ex));
    }

    /**
     * Try to copy exception using ExceptionUtils.copyExceptionWithCause and return new exception if it was not possible.
     *
     * @param e Exception.
     * @return Properly copied exception or a new error, if exception can not be copied.
     */
    // TODO: consider removing after IGNITE-22721 gets resolved.
    private static <T extends Throwable & TraceableException> Throwable copyExceptionWithCauseIfPossible(T e) {
        // Copy exception with cause does not respect custom exception fields.
        // We should just create this during compile time and call it a day, or at least cache this. Create ticket.
        try {
            Class<T> klass = (Class<T>) e.getClass();
            MethodHandles.Lookup privateLookup = MethodHandles.privateLookupIn(klass, MethodHandles.lookup());
            MethodHandle mhandle = privateLookup.findStatic(
                    klass,
                    "copy",
                    MethodType.methodType(klass, klass) // adjust signature
            );

            return (T) mhandle.invoke(e);
        } catch (Throwable ignored) {
            // Intentionally left blank.
        }

        Throwable copy = ExceptionUtils.copyExceptionWithCause(e.getClass(), e.traceId(), e.code(), e.getMessage(), e.getCause());
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
