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

package org.apache.ignite.internal.lang;

import static java.util.Collections.unmodifiableMap;
import static org.apache.ignite.internal.util.CompletableFutures.isCompletedSuccessfully;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;

/**
 * This utility class provides an ability to map Ignite internal exceptions to Ignite public ones.
 */
public class IgniteExceptionMapperUtil {
    /** All exception mappers to be used to map internal exceptions to public ones. */
    private static final Map<Class<? extends Exception>, IgniteExceptionMapper<?, ?>> EXCEPTION_CONVERTERS;

    static {
        Map<Class<? extends Exception>, IgniteExceptionMapper<?, ?>> mappers = new HashMap<>();

        ServiceLoader
                .load(IgniteExceptionMappersProvider.class)
                .forEach(provider -> provider.mappers().forEach(m -> registerMapping(m, mappers)));

        EXCEPTION_CONVERTERS = unmodifiableMap(mappers);
    }

    /**
     * Add a new mapping to already registered ones.
     *
     * @param mapper Exception mapper from internal exception to a public one.
     * @param registeredMappings Already registered mappings.
     * @throws IgniteException If a mapper for the given {@code clazz} already registered, or {@code clazz} represents Java standard
     *         exception like {@link NullPointerException}, {@link IllegalArgumentException}.
     */
    static void registerMapping(
            IgniteExceptionMapper<?, ?> mapper,
            Map<Class<? extends Exception>, IgniteExceptionMapper<?, ?>> registeredMappings) {
        if (registeredMappings.containsKey(mapper.mappingFrom())) {
            throw new IgniteException(
                    INTERNAL_ERR,
                    "Failed to register exception mapper, duplicate found [class=" + mapper.mappingFrom().getCanonicalName() + ']');
        }

        registeredMappings.put(mapper.mappingFrom(), mapper);
    }

    /**
     * This method provides a mapping from internal exception to Ignite public ones.
     *
     * <p>The rules of mapping are the following:</p>
     * <ul>
     *     <li>any instance of {@link Error} is returned as is, except {@link AssertionError}
     *     that will always be mapped to {@link IgniteException} with the {@link Common#INTERNAL_ERR} error code.</li>
     *     <li>any instance of {@link IgniteException} or {@link IgniteCheckedException} is returned as is.</li>
     *     <li>if there are no any mappers that can do a mapping from the given error to a public exception,
     *     then {@link IgniteException} with the {@link Common#INTERNAL_ERR} error code is returned.</li>
     * </ul>
     *
     * @param origin Exception to be mapped.
     * @return Public exception.
     */
    public static Throwable mapToPublicException(Throwable origin) {
        if (origin instanceof Error) {
            if (origin instanceof AssertionError) {
                return new IgniteException(INTERNAL_ERR, origin);
            }
            return origin;
        }

        Throwable res;

        // Try to find appropriate mapper, moving from original class to supper-classes step by step.
        Class exceptionClass = origin.getClass();
        IgniteExceptionMapper<? extends Exception, ? extends Exception> m;
        while ((m = EXCEPTION_CONVERTERS.get(exceptionClass)) == null && exceptionClass != Throwable.class) {
            exceptionClass = exceptionClass.getSuperclass();
        }

        if (m != null) {
            res = map(m, origin);

            assert res instanceof IgniteException || res instanceof IgniteCheckedException :
                    "Unexpected mapping of internal exception to a public one [origin=" + origin + ", mapped=" + res + ']';

        } else {
            res = origin;
        }

        if (res instanceof IgniteException || res instanceof IgniteCheckedException) {
            return res;
        }

        // There are no exception mappings for the given exception. This case should be considered as internal error.
        return new IgniteException(INTERNAL_ERR, origin);
    }

    /**
     * Returns a new CompletableFuture that, when the given {@code origin} future completes exceptionally, maps the origin's exception to a
     * public Ignite exception if it is needed.
     *
     * @param origin The future to use to create a new stage.
     * @param <T> Type os result.
     * @return New CompletableFuture.
     */
    public static <T> CompletableFuture<T> convertToPublicFuture(CompletableFuture<T> origin) {
        if (isCompletedSuccessfully(origin)) {
            // No need to translate exceptions.
            return origin;
        }

        return origin
                .handle((res, err) -> {
                    if (err != null) {
                        throw new CompletionException(mapToPublicException(unwrapCause(err)));
                    }

                    return res;
                });
    }

    /**
     * Returns a new instance of public exception provided by the {@code mapper}.
     *
     * @param mapper Mapper function to produce a public exception.
     * @param t Internal exception.
     * @param <T> Type of an internal exception.
     * @param <R> Type of a public exception.
     * @return New public exception.
     */
    private static <T extends Exception, R extends Exception> Exception map(IgniteExceptionMapper<T, R> mapper, Throwable t) {
        return mapper.map(mapper.mappingFrom().cast(t));
    }
}
