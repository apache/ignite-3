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

package org.apache.ignite.table;

/**
 * Data streamer item: data and operation type.
 */
public interface DataStreamerItem<T> {
    /**
     * Gets the data.
     *
     * @return Data.
     */
    T get();

    /**
     * Gets the operation type.
     *
     * @return Operation type.
     */
    DataStreamerOperationType operationType();

    /**
     * Creates a new data streamer item with the given data and {@link DataStreamerOperationType#PUT} operation type.
     *
     * @param item Data.
     * @param <T> Data type.
     *
     * @return PUT data streamer item.
     */
    static <T> DataStreamerItem<T> of(T item) {
        return instance(item, DataStreamerOperationType.PUT);
    }

    /**
     * Creates a new data streamer item with the given data and {@link DataStreamerOperationType#REMOVE} operation type.
     *
     * @param item Data.
     * @param <T> Data type.
     *
     * @return REMOVE data streamer item.
     */
    static <T> DataStreamerItem<T> removed(T item) {
        return instance(item, DataStreamerOperationType.REMOVE);
    }

    private static <T> DataStreamerItem<T> instance(T item, DataStreamerOperationType operationType) {
        return new DataStreamerItem<>() {
            @Override
            public T get() {
                return item;
            }

            @Override
            public DataStreamerOperationType operationType() {
                return operationType;
            }
        };
    }
}