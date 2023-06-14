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

package org.apache.ignite.internal.storage.index;

import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * {@link Cursor} extension with the ability to {@link #peek() peek} at the next element.
 */
public interface PeekCursor<T> extends Cursor<T> {
    /**
     * Returns the next element without advancing the cursor, {@code null} if there is no next element.
     *
     * <p>The behavior of this method is tightly coupled with the behaviour of {@link #next()} and {@link #hasNext()}:
     * <ul>
     *     <li>
     *         {@code peek()} returns exactly the same value as the {@link #next()} would, if called instead or right after.
     *     </li>
     *     <li>
     *         This property can be applied to the end of the cursor. {@code peek()} returns {@code null} if {@link #next()} would
     *         throw an {@link java.util.NoSuchElementException}, if it was called instead or right after.
     *     </li>
     *     <li>
     *         {@link #next()}, called immediately after {@code peek()}, must thus return the same value, or throw an exception if the
     *         value was {@code null}.
     *     </li>
     * </ul>
     * These properties, in conjunction with basic iterator invariants, give strict constraints on {@link #hasNext()} usage:
     * <ul>
     *     <li>After the cursor is created, {@code #peek()} will return the actual (up-to-date) next element;</li>
     *     <li>After calling {@link #hasNext()}, if it returned {@code true}, then {@code peek()} will return the element (cached) that
     *     {@link #next()} would return, but without advancing the cursor;</li>
     *     <li>After calling {@link #hasNext()}, if it returned {@code false}, then {@code peek()} will always return {@code null};</li>
     *     <li>After {@link #next()} is called, but before {@link #hasNext()} is called, {@code peek()} will always return the actual
     *     (up-to-date) next element without advancing the cursor.</li>
     * </ul>
     * In other words, {@link #hasNext()} call forces the cursor to return cached value from {@code peek()}, instead of looking for
     * up-to-date value in the storage.
     */
    @Nullable T peek();
}
