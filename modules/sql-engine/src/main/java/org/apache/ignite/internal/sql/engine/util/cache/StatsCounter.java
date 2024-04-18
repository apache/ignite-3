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
 * Accumulates statistics during the operation of a {@link Cache}. This is solely intended for consumption by {@code Cache} implementors.
 */
public interface StatsCounter {
    /**
     * Records cache hits. This should be called when a cache request returns a cached value.
     *
     * @param count the number of hits to record
     */
    void recordHits(int count);

    /**
     * Records cache misses. This should be called when a cache request returns a value that was not found in the cache.
     *
     * @param count the number of misses to record
     */
    void recordMisses(int count);
}
