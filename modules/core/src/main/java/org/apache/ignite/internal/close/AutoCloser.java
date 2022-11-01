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

package org.apache.ignite.internal.close;

/**
 * A utility that allows to use something that is not an {@link AutoCloseable} in try-with-resources blocks.
 *
 * @param <T> Type of the resource.
 * @param <X> Exception that can be thrown by the closer during closing. This type parameter is needed to make sure
 *     that {@link AutoCloser#close()} throws declaration is as narrow as possible.
 */
@SuppressWarnings("CheckedExceptionClass")
public class AutoCloser<T, X extends Exception> implements AutoCloseable {
    private final T value;

    private final Closer<? super T, ? extends X> closer;

    /**
     * Constructs an AutoCloser from a value (that needs to be closed) and a closer (that will be used to close the value).
     *
     * @param value Thing that needs to be eventually closed.
     * @param closer Knows how to close the thing.
     */
    public AutoCloser(T value, Closer<? super T, ? extends X> closer) {
        this.value = value;
        this.closer = closer;
    }

    /**
     * Returns the wrapped value.
     *
     * @return Wrapped value.
     */
    public T value() {
        return value;
    }

    @Override
    public void close() throws X {
        closer.close(value);
    }

    /**
     * Used to close the wrapped value.
     *
     * @param <T> Type of the resource.
     * @param <X> Exception that can be thrown by the closer during closing. This type parameter is needed to make sure
     *     that {@link AutoCloser#close()} throws declaration is as narrow as possible.
     */
    @FunctionalInterface
    public interface Closer<T, X extends Exception> {
        /**
         * Closes a value.
         *
         * @param value Value to be closed.
         * @throws X Thrown if something goes wrong when closing.
         */
        void close(T value) throws X;
    }
}
