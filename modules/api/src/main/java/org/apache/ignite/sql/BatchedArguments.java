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

package org.apache.ignite.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Arguments for batch query execution.
 */
public final class BatchedArguments implements Iterable<List<Object>> {
    /** Batched arguments. */
    private final List<List<Object>> batchedArgs;

    /**
     * Creates batched arguments.
     *
     * @return Batch query arguments.
     */
    public static BatchedArguments create() {
        return new BatchedArguments(new ArrayList<>());
    }

    /**
     * Creates batched arguments.
     *
     * @param args Arguments.
     * @return Batch query arguments.
     */
    public static BatchedArguments of(Object... args) {
        BatchedArguments arguments = create();

        arguments.add(args);

        return arguments;
    }

    /**
     * Creates batched arguments.
     *
     * @param batchedArgs Arguments.
     * @return Batch query arguments.
     */
    public static BatchedArguments of(List<List<Object>> batchedArgs) {
        Objects.requireNonNull(batchedArgs, "batchedArgs");

        return new BatchedArguments(batchedArgs);
    }

    /**
     * Appends arguments to a batch.
     *
     * @param args Arguments.
     * @return {@code this} for chaining.
     */
    public BatchedArguments add(Object... args) {
        Objects.requireNonNull(args, "args");

        return add(List.of(args));
    }

    /**
     * Appends arguments to a batch.
     *
     * @param argsList Arguments list.
     * @return {@code this} for chaining.
     */
    public BatchedArguments add(List<Object> argsList) {
        Objects.requireNonNull(argsList, "argsList");

        if (argsList.isEmpty()) {
            throwEmptyArgumentsException();
        }

        if (!batchedArgs.isEmpty()) {
            ensureRowLength(batchedArgs.get(0).size(), argsList.size());
        }

        batchedArgs.add(argsList);

        return this;
    }

    /**
     * Returns the arguments list at the specified position.
     *
     * @param index index of the element to return.
     * @return Arguments list.
     */
    public List<Object> get(int index) {
        return batchedArgs.get(index);
    }

    /**
     * Returns the size of this batch.
     *
     * @return Batch size.
     */
    public int size() {
        return batchedArgs.size();
    }

    /**
     * Returns {@code true} if this batch contains is empty.
     *
     * @return {@code True} if this batch contains is empty.
     */
    public boolean isEmpty() {
        return batchedArgs.isEmpty();
    }

    /**
     * Returns an iterator over the elements in this batch.
     *
     * @return Iterator over the elements in this batch.
     */
    @Override
    public Iterator<List<Object>> iterator() {
        return new ImmutableIterator<>(batchedArgs.iterator());
    }

    /** Constructor. */
    private BatchedArguments(List<List<Object>> batchedArgs) {
        if (!batchedArgs.isEmpty()) {
            int pos = 0;
            int requiredLength = 0;

            for (List<Object> arguments : batchedArgs) {
                Objects.requireNonNull(arguments, "Arguments list cannot be null.");

                if (arguments.isEmpty()) {
                    throwEmptyArgumentsException();
                }

                if (pos == 0) {
                    requiredLength = arguments.size();
                } else {
                    ensureRowLength(requiredLength, arguments.size());
                }

                ++pos;
            }
        }

        this.batchedArgs = batchedArgs;
    }

    private static void throwEmptyArgumentsException() {
        throw new IllegalArgumentException("Non empty arguments required.");
    }

    private static void ensureRowLength(int expected, int actual) {
        if (expected != actual) {
            throw new IllegalArgumentException("Argument lists must be the same size.");
        }
    }

    private static class ImmutableIterator<E> implements Iterator<E> {
        private final Iterator<E> delegate;

        private ImmutableIterator(Iterator<E> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public E next() {
            return delegate.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            delegate.forEachRemaining(action);
        }
    }
}
