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

package org.apache.ignite.internal.sql.engine.util;

import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;

/**
 * Factory for creating a function to calculate the hash of the specified fields of the row.
 */
public interface HashFunctionFactory<T> {
    /**
     * A function to calculate hash of given row.
     *
     * @param <T> A type of the row.
     */
    @FunctionalInterface
    interface RowHashFunction<T> {
        /**
         * Calculates hash of given row.
         *
         * @param row A row to calculate hash for.
         * @return A hash of the row.
         */
        int hashOf(T row);
    }

    /**
     * Creates a hash function to compute a composite hash of a row, given the values of the fields.
     *
     * @param fields Field ordinals of the row from which the hash is to be calculated.
     * @return Function to compute a composite hash of a row, given the values of the fields.
     */
    RowHashFunction<T> create(int[] fields);

    /**
     * Creates a hash function to compute a composite hash of a row, given the types and values of the fields.
     *
     * @param fields Field ordinals of the row from which the hash is to be calculated.
     * @param tableDescriptor Table descriptor.
     * @return Function to compute a composite hash of a row, given the types and values of the fields.
     */
    RowHashFunction<T> create(int[] fields, TableDescriptor tableDescriptor);
}
