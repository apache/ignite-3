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

package org.apache.ignite.internal.deployunit.metastore;

import org.apache.ignite.internal.metastorage.Entry;

/**
 * Values accumulator. Implementation should NOT be thead-safe.
 *
 * @param <R> Result value type.
 */
public interface Accumulator<R> {
    /**
     * Accumulate provided value.
     *
     * @param item Item from metastore.
     */
    void accumulate(Entry item);

    /**
     * Returns all accumulated values transformed to required type.
     *
     * @return All accumulated values transformed to required type.
     * @throws AccumulateException in case when accumulation or transformation failed.
     */
    R get() throws AccumulateException;
}
