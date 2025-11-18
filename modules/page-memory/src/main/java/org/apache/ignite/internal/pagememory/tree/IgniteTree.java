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

package org.apache.ignite.internal.pagememory.tree;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for ignite internal tree.
 *
 * @param <L> Type of keys maintained by this tree.
 * @param <T> Type of mapped values.
 */
// TODO: IGNITE-16350 Check if the interface is needed and remove if not needed.
public interface IgniteTree<L, T> {
    /**
     * Put value in this tree.
     *
     * @param val Value to be associated with the specified key.
     * @return The previous value associated with key.
     * @throws IgniteInternalCheckedException If failed.
     */
    @Nullable T put(T val) throws IgniteInternalCheckedException;

    /**
     * Invokes a closure for a value by given key.
     *
     * @param key Key.
     * @param x Implementation specific argument, {@code null} always means that we need a full detached data row.
     * @param c Closure.
     * @throws IgniteInternalCheckedException If failed.
     */
    void invoke(L key, @Nullable Object x, InvokeClosure<T> c) throws IgniteInternalCheckedException;

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this tree contains no mapping for the key.
     *
     * @param key The key whose associated value is to be returned.
     * @throws IgniteInternalCheckedException If failed.
     */
    @Nullable T findOne(L key) throws IgniteInternalCheckedException;

    /**
     * Returns a cursor from lower to upper bounds inclusive.
     *
     * @param lowerInclusive Lower bound (inclusive) or {@code null} if unbounded.
     * @param upperInclusive Upper bound (inclusive) or {@code null} if unbounded.
     * @throws IgniteInternalCheckedException If failed.
     */
    Cursor<T> find(@Nullable L lowerInclusive, @Nullable L upperInclusive) throws IgniteInternalCheckedException;

    /**
     * Returns a cursor from lower to upper bounds inclusive.
     *
     * @param lowerInclusive Lower bound (inclusive) or {@code null} if unbounded.
     * @param upperInclusive Upper bound (inclusive) or {@code null} if unbounded.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @throws IgniteInternalCheckedException If failed.
     */
    Cursor<T> find(@Nullable L lowerInclusive, @Nullable L upperInclusive, @Nullable Object x) throws IgniteInternalCheckedException;

    /**
     * Returns a value mapped to the lowest key, or {@code null} if tree is empty.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    @Nullable T findFirst() throws IgniteInternalCheckedException;

    /**
     * Returns a value mapped to the greatest key, or {@code null} if tree is empty.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    @Nullable T findLast() throws IgniteInternalCheckedException;

    /**
     * Removes the mapping for a key from this tree if it is present.
     *
     * @param key Key whose mapping is to be removed from the tree.
     * @return The previous value associated with key, or {@code null} if there was no mapping for key.
     * @throws IgniteInternalCheckedException If failed.
     */
    @Nullable T remove(L key) throws IgniteInternalCheckedException;

    /**
     * Returns the number of elements in this tree.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    long size() throws IgniteInternalCheckedException;

    /**
     * Closure to perform an operation on a row.
     */
    interface InvokeClosure<T> {
        /**
         * Performs this operation on the given row.
         *
         * @param oldRow Old row or {@code null} if old row not found.
         * @throws IgniteInternalCheckedException If failed.
         */
        void call(@Nullable T oldRow) throws IgniteInternalCheckedException;

        /**
         * Returns new row for {@link OperationType#PUT} operation.
         */
        @Nullable T newRow();

        /**
         * Returns operation type for this closure.
         */
        OperationType operationType();

        /**
         * Callback after inserting/replacing/deleting a tree row.
         *
         * <p>It is performed under the same write lock of page on which the tree row is located.
         *
         * <p>What can allow us to ensure the atomicity of changes in the tree row and the data associated with it.
         */
        default void onUpdate() {
            // No-op.
        }
    }

    /**
     * Operation type.
     */
    enum OperationType {
        NOOP,

        REMOVE,

        PUT,

        IN_PLACE
    }
}
