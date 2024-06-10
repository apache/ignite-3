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

import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;

/** Index status as stored in the {@link IndexMeta}. */
public enum MetaIndexStatus {
    /**
     * Index has been created in the catalog.
     *
     * @see CatalogIndexStatus#REGISTERED
     */
    REGISTERED,

    /**
     * Index was in the process of being built.
     *
     * @see CatalogIndexStatus#BUILDING
     */
    BUILDING,

    /**
     * Index has been successfully built and is available for reading from it.
     *
     * @see CatalogIndexStatus#AVAILABLE
     */
    AVAILABLE,

    /**
     * Available index has started preparing to be removed from the catalog.
     *
     * @see CatalogIndexStatus#STOPPING
     */
    STOPPING,

    /**
     * Index has been removed from the catalog, with {@link CatalogIndexStatus#REGISTERED}/{@link CatalogIndexStatus#BUILDING} statuses, but
     * has not yet been destroyed due to a low watermark.
     */
    // TODO: IGNITE-20934 Get rid of such indexes immediately
    REMOVED,

    /**
     * Index has been removed from the catalog, with statuses {@link CatalogIndexStatus#AVAILABLE} (on table destruction) or
     * {@link CatalogIndexStatus#STOPPING}, but has not yet been destroyed due to a low watermark.
     *
     * <p>Such an index cannot be used by RW transactions, only by RO transactions with the correct readTimestamp.</p>
     */
    READ_ONLY;

    /** Converts {@link CatalogIndexStatus} to {@link MetaIndexStatus}. */
    static MetaIndexStatus convert(CatalogIndexStatus catalogIndexStatus) {
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
