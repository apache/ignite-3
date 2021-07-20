/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration;

import java.util.List;

/** */
public interface NamedListView<T> {
    /**
     * @return Immutable collection of keys contained within this list.
     */
    List<String> namedListKeys();

    /**
     * Returns value associated with the passed key.
     *
     * @param key Key string.
     * @return Requested value or {@code null} if it's not found.
     */
    T get(String key);

    /**
     * Returns value located at the specified index.
     *
     * @param index Value index.
     * @return Requested value.
     * @throws IndexOutOfBoundsException If index is out of bounds.
     */
    T get(int index) throws IndexOutOfBoundsException;

    /**
     * @return Number of elements.
     */
    int size();
}
