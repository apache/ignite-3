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

package org.apache.ignite.internal.table.distributed.index;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;

/** Index status as stored in the {@link IndexMeta}. */
public enum MetaIndexStatus {
    /**
     * Index has been created in the catalog.
     *
     * @see CatalogIndexStatus#REGISTERED
     */
    REGISTERED(0),

    /**
     * Index was in the process of being built.
     *
     * @see CatalogIndexStatus#BUILDING
     */
    BUILDING(1),

    /**
     * Index has been successfully built and is available for reading from it.
     *
     * @see CatalogIndexStatus#AVAILABLE
     */
    AVAILABLE(2),

    /**
     * Available index has started preparing to be removed from the catalog.
     *
     * @see CatalogIndexStatus#STOPPING
     */
    STOPPING(3),

    /**
     * Index has been removed from the catalog, with {@link CatalogIndexStatus#REGISTERED}/{@link CatalogIndexStatus#BUILDING} statuses, but
     * has not yet been destroyed due to a low watermark.
     */
    // TODO: IGNITE-20934 Get rid of such indexes immediately
    REMOVED(4),

    /**
     * Index has been removed from the catalog, with statuses {@link CatalogIndexStatus#AVAILABLE} (on table destruction) or
     * {@link CatalogIndexStatus#STOPPING}, but has not yet been destroyed due to a low watermark.
     *
     * <p>Such an index cannot be used by RW transactions, only by RO transactions with the correct readTimestamp.</p>
     */
    READ_ONLY(5);

    /** Status code. It's persisted and should not be changed for existing statuses. */
    private final int code;

    private static final MetaIndexStatus[] VALUES_INDEXED_BY_CODE;

    static {
        int maxCode = -1;
        Set<Integer> seenCodes = new HashSet<>();
        for (MetaIndexStatus status : values()) {
            assert status.code >= 0 : status + " has a negative code";
            maxCode = Math.max(maxCode, status.code);

            boolean added = seenCodes.add(status.code);
            assert added : "Duplicate status code for: " + status;
        }

        VALUES_INDEXED_BY_CODE = new MetaIndexStatus[maxCode + 1];
        for (MetaIndexStatus status : values()) {
            VALUES_INDEXED_BY_CODE[status.code] = status;
        }
    }

    MetaIndexStatus(int code) {
        this.code = code;
    }

    /**
     * Finds status by code.
     *
     * @param code Code to use when searching.
     * @throws IllegalArgumentException If there is no status with the provided code.
     */
    static MetaIndexStatus findByCode(int code) {
        MetaIndexStatus status = VALUES_INDEXED_BY_CODE[code];

        if (status == null) {
            throw new IllegalArgumentException("Unknown code [" + code + "]");
        }

        return status;
    }

    /** Returns status code (the code is persisted and should not be changed for existing statuses). */
    public int code() {
        return code;
    }

    /** Converts {@link CatalogIndexStatus} to {@link MetaIndexStatus}. */
    public static MetaIndexStatus convert(CatalogIndexStatus catalogIndexStatus) {
        switch (catalogIndexStatus) {
            case REGISTERED:
                return REGISTERED;
            case BUILDING:
                return BUILDING;
            case AVAILABLE:
                return AVAILABLE;
            case STOPPING:
                return STOPPING;
            default:
                throw new AssertionError("Unknown status: " + catalogIndexStatus);
        }
    }

    /** Returns the index status on an index removal event from a catalog, based on the previous status. */
    static MetaIndexStatus statusOnRemoveIndex(MetaIndexStatus previousStatus) {
        switch (previousStatus) {
            case REGISTERED:
            case BUILDING:
                return REMOVED;
            case AVAILABLE:
            case STOPPING:
                return READ_ONLY;
            default:
                throw new AssertionError("Unknown status: " + previousStatus);
        }
    }
}
