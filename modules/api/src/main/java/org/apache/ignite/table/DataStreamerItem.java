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
 * Data streamer item. See {@link DataStreamerTarget}.
 *
 * @param <T> Data type.
 */
public interface DataStreamerItem<T> {
    /**
     * Gets the underlying data.
     *
     * @return Data item.
     */
    T get();

    /**
     * Gets a value indicating whether the item is deleted.
     *
     * @return {@code true} if the item is deleted, {@code false} otherwise.
     */
    boolean isRemoved();

    static <T> DataStreamerItem<T> of(T item) {
        return of(item, false);
    }

    static <T> DataStreamerItem<T> removed(T item) {
        return of(item, true);
    }

    static <T> DataStreamerItem<T> of(T item, boolean isRemoved) {
        return new DataStreamerItem<T>() {
            @Override
            public T get() {
                return item;
            }

            @Override
            public boolean isRemoved() {
                return isRemoved;
            }
        };
    }
}
