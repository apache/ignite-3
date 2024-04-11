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
 *     <li>[not-existent] -> {@link #REGISTERED} -> {@link #BUILDING} -> {@link #AVAILABLE}.</li>
 *     <li>[not-existent] -> {@link #AVAILABLE} (PK index).</li>
 *     <li>{@link #AVAILABLE} -> {@link #STOPPING} -> [removed]. (when dropping an index, but not its table)</li>
 *     <li>{@link #AVAILABLE} -> [removed]. (when dropping the table of an index)</li>
 *     <li>{@link #REGISTERED} -> [removed].</li>
 *     <li>{@link #BUILDING} -> [removed].</li>
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
    AVAILABLE(2),

    /**
     * DROP INDEX command has been executed, the index is waiting for RW transactions started when the index was {@link #AVAILABLE}
     * to finish. After the wait is finished, the index will automatically be removed from the Catalog.
     *
     * <p>New RW transactions cannot read the index, but they write to it. RO transactions can still read from it if the readTimestamp
     * corresponds to a moment when the index was still {@link #AVAILABLE}.</p>
     */
    STOPPING(3);

    private static final CatalogIndexStatus[] VALS = new CatalogIndexStatus[values().length];

    private final int id;

    CatalogIndexStatus(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    /**
     * Returns {@code true} if this index is alive, i.e. not in the {@link #STOPPING} state.
     */
    public boolean isAlive() {
        return this != STOPPING;
    }

    /**
     * Returns {@code true} if the index is {@link #AVAILABLE} or follows it.
     */
    public boolean isAvailableOrLater() {
        switch (this) {
            case AVAILABLE:
            case STOPPING:
                return true;
            default:
                return false;
        }
    }

    static {
        for (CatalogIndexStatus status : values()) {
            assert VALS[status.id] == null : "Found duplicate id " + status.id;

            VALS[status.id()] = status;
        }
    }

    /** Returns index status by identifier. */
    static CatalogIndexStatus forId(int id) {
        if (id >= 0 && id < VALS.length) {
            return VALS[id];
        }

        throw new IllegalArgumentException("Incorrect index status identifier: " + id);
    }
}
