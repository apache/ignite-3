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

import java.util.function.Function;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;

/**
 * Represents a mapper from an internal exception {@code T} to a public one {@code R}.
 */
public class IgniteExceptionMapper<T extends Exception, R extends Exception> {
    /** Class that represents an internal exception for mapping. */
    private final Class<T> from;

    /** Mapping function. */
    private final Function<T, R> mapper;

    /**
     * Creates a new instance of mapper.
     *
     * @param from Class instance that represents a class of internal exception.
     * @param mapper Mapping function to map an internal exception to a public one.
     */
    private IgniteExceptionMapper(Class<T> from, Function<T, R> mapper) {
        this.from = from;
        this.mapper = mapper;
    }

    /**
     * Returns a class instance that represents an internal exception to be used for mapping.
     *
     * @return Class instance that represents an internal exception to be used for mapping.
     */
    public Class<T> mappingFrom() {
        return from;
    }

    /**
     * Maps the provided internal exception to a public one.
     *
     * @param exception Exception instance to be mapped.
     * @return Public exception instance.
     */
    public R map(T exception) {
        return mapper.apply(exception);
    }

    /**
     * Creates a new exception mapper from an internal exception {@code T} to a public runtime exception {@code R}.
     *
     * @param from Class instance that represents a class of internal exception.
     * @param mapper Mapping function to map an internal exception to a public one.
     * @param <T> Internal exception type.
     * @param <R> Public runtime exception type.
     *
     * @return New instance of {@link IgniteExceptionMapper}.
     */
    public static <T extends Exception, R extends IgniteException> IgniteExceptionMapper<T, R> unchecked(
            Class<T> from,
            Function<T, R> mapper
    ) {
        return new IgniteExceptionMapper<T, R>(from, mapper);
    }

    /**
     * Creates a new exception mapper from an internal exception {@code T} to a public checked exception {@code R}.
     *
     * @param from Class instance that represents a class of internal exception.
     * @param mapper Mapping function to map an internal exception to a public one.
     * @param <T> Internal exception type.
     * @param <R> Public checked exception type.
     *
     * @return New instance of {@link IgniteExceptionMapper}.
     */
    public static <T extends Exception, R extends IgniteCheckedException> IgniteExceptionMapper<T, R> checked(
            Class<T> from,
            Function<T, R> mapper
    ) {
        return new IgniteExceptionMapper<T, R>(from, mapper);
    }
}
