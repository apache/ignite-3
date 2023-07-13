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

/**
 * Indicates incompatible schema version.
 */
public final class SchemaVersionMismatchException extends SchemaException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    private final int expectedVersion;

    private final int actualVersion;

    public SchemaVersionMismatchException(int expectedVersion, int actualVer) {
        super("Schema version mismatch [expectedVer=" + expectedVersion + ", actualVer=" + actualVer + ']');

        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVer;
    }

    public int expectedVer() {
        return expectedVersion;
    }

    public int actualVer() {
        return actualVersion;
    }
}
