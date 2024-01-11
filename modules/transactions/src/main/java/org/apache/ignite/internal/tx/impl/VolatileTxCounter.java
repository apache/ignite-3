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

package org.apache.ignite.internal.tx.impl;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Volatile transaction counter by catalog version.
 *
 * <p>Thread safe.</p>
 */
class VolatileTxCounter {
    private final NavigableMap<Integer, Long> txCounterByCatalogVersion = new ConcurrentSkipListMap<>();

    /**
     * Increments the transaction counter.
     *
     * @param catalogVersion Catalog version on which the transaction was created.
     */
    void incrementTxCount(int catalogVersion) {
        txCounterByCatalogVersion.compute(catalogVersion, (i, txCount) -> {
            if (txCount == null) {
                return 1L;
            }

            return txCount + 1L;
        });
    }

    /**
     * Decrements the transaction counter.
     *
     * @param catalogVersion Catalog version on which the transaction was created.
     */
    void decrementTxCount(int catalogVersion) {
        txCounterByCatalogVersion.compute(catalogVersion, (i, txCount) -> {
            assert txCount != null : catalogVersion;

            return txCount == 1 ? null : txCount - 1L;
        });
    }

    /**
     * Checks whether there are transactions before the requested catalog version.
     *
     * @param toCatalogVersion Catalog version (excluding) to check up to.
     */
    boolean isExistsTxBefore(int toCatalogVersion) {
        return !txCounterByCatalogVersion.headMap(toCatalogVersion).isEmpty();
    }

    /** Clears all counters. */
    void clear() {
        txCounterByCatalogVersion.clear();
    }
}
