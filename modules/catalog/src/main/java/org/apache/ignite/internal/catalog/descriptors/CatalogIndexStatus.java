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
 * Index status.
 *
 * <p>Possible status transitions:</p>
 * <ul>
 *     <li>{@link #REGISTERED} -> {@link #BUILDING} -> {@link #AVAILABLE}.</li>
 *     <li>{@link #AVAILABLE} (PK index).</li>
 * </ul>
 */
public enum CatalogIndexStatus {
    /**
     * Index has been registered and is awaiting the start of building.
     *
     * <p>Write only.</p>
     */
    REGISTERED(0),

    /**
     * Index is in the process of being built.
     *
     * <p>Write only.</p>
     */
    BUILDING(1),

    /**
     * Index is built and ready to use.
     *
     * <p>Readable and writable.</p>
     */
    AVAILABLE(2);

    private static final CatalogIndexStatus[] VALS = new CatalogIndexStatus[values().length];

    private final int id;

    CatalogIndexStatus(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    static {
        for (CatalogIndexStatus status : values()) {
            assert VALS[status.id] == null : "Found duplicate id " + status.id;

            VALS[status.id()] = status;
        }
    }

    /** Returns index status by identifier. */
    public static CatalogIndexStatus getById(int id) {
        assert id >= 0 && id < VALS.length : "id=" + id;

        return VALS[id];
    }
}
