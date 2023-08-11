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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.lang.ErrorGroups.Table.SCHEMA_VERSION_MISMATCH_ERR;

/**
 * Indicates incompatible schema version.
 */
public final class SchemaVersionMismatchException extends SchemaException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Expected schema version. */
    private final int expectedVersion;

    /** Actual schema version. */
    private final int actualVersion;

    /**
     * Constructor.
     *
     * @param expectedVersion Expected schema version.
     * @param actualVer Actual schema version.
     */
    public SchemaVersionMismatchException(int expectedVersion, int actualVer) {
        super(SCHEMA_VERSION_MISMATCH_ERR, "Schema version mismatch [expectedVer=" + expectedVersion + ", actualVer=" + actualVer + ']');

        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVer;
    }

    /**
     * Gets expected schema version.
     *
     * @return Expected schema version.
     */
    public int expectedVersion() {
        return expectedVersion;
    }

    /**
     * Gets actual schema version.
     *
     * @return Actual schema version.
     */
    public int actualVersion() {
        return actualVersion;
    }
}
