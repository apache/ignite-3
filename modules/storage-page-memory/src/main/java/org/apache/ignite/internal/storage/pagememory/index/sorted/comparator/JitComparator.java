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

package org.apache.ignite.internal.storage.pagememory.index.sorted.comparator;

import org.apache.ignite.internal.schema.UnsafeByteBufferAccessor;

/**
 * Interface for comparing two binary tuples, represented as {@link UnsafeByteBufferAccessor}s. All implementation of this interface are
 * expected to be generated in runtime by {@link JitComparatorGenerator}.
 */
@FunctionalInterface
public interface JitComparator {
    /**
     * Compares two binary tuples, represented as {@link UnsafeByteBufferAccessor}s.
     *
     * @param outerAccessor First tuple accessor.
     * @param outerSize First tuple size.
     * @param innerAccessor Second tuple accessor.
     * @param innerSize Second tuple size.
     * @return Comparison result.
     */
    int compare(UnsafeByteBufferAccessor outerAccessor, int outerSize, UnsafeByteBufferAccessor innerAccessor, int innerSize);
}
