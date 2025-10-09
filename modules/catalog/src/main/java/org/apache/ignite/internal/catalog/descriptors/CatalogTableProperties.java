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

package org.apache.ignite.internal.catalog.descriptors;

/**
 * This class encapsulates different properties of a table.
 *
 * <p>Put here configuration that is required by auxiliary systems, but not defines how and where to store the data.
 */
public class CatalogTableProperties {
    private final double staleRowsFraction;
    private final long minStaleRowsCount;

    CatalogTableProperties(double staleRowsFraction, long minStaleRowsCount) {
        this.staleRowsFraction = staleRowsFraction;
        this.minStaleRowsCount = minStaleRowsCount;
    }

    /**
     * Returns {@code double} value in the range [0.0, 1] representing fraction of a partition to be modified before the data is considered
     * to be "stale". That is, any computation made on a data snapshot is considered obsolete and need to be refreshed.
     */
    public double staleRowsFraction() {
        return staleRowsFraction;
    }

    /**
     * Returns minimal number of rows in partition to be modified before the data is considered to be "stale". That is, any computation made
     * on a data snapshot is considered obsolete and need to be refreshed.
     */
    public long minStaleRowsCount() {
        return minStaleRowsCount;
    }
}
