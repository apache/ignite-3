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

package org.apache.ignite.internal.sql.engine.util.cache;

/**
 * Factory that creates a cache.
 */
public interface CacheFactory {
    /**
     * Creates a cache with a desired size.
     *
     * @param size Desired size of the cache.
     * @return An instance of the cache.
     * @param <K> Type of the key object.
     * @param <V> Type of the value object.
     */
    <K, V> Cache<K, V> create(int size);

    /**
     * Creates a cache of the required size and controls whether statistics should be collected.
     *
     * @param size Desired size of the cache.
     * @param statCounter Cache statistic accumulator.
     * @return An instance of the cache.
     * @param <K> Type of the key object.
     * @param <V> Type of the value object.
     */
    <K, V> Cache<K, V> create(int size, StatsCounter statCounter);
}
