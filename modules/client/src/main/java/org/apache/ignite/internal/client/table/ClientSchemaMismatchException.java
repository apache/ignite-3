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

package org.apache.ignite.internal.client.table;

import java.util.UUID;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Client schema mismatch exception.
 */
public class ClientSchemaMismatchException extends IgniteInternalException {
    private static final long serialVersionUID = -7580614146202932937L;

    private final int schemaVersion;

    /**
     * Constructor.
     *
     * @param schemaVersion Schema version.
     */
    public ClientSchemaMismatchException(int schemaVersion) {
        super(UUID.randomUUID(), ErrorGroups.Client.SCHEMA_MISMATCH, "Data does not match the schema version: " + schemaVersion);

        this.schemaVersion = schemaVersion;
    }

    /**
     * Gets the current schema version.
     *
     * @return Schema version.
     */
    public int schemaVersion() {
        return schemaVersion;
    }
}
