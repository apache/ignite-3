/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.table;

import java.util.UUID;
import org.apache.ignite.internal.manager.EventParameters;
import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 * Table event parameter.
 */
public class SchemaEventParameters extends EventParameters {
    /** Table identifier. */
    private final UUID tableId;

    /** Schema descriptor. */
    private final SchemaDescriptor schemaDescriptor;

    public SchemaEventParameters(long causalityToken, UUID tableId, SchemaDescriptor schemaDescriptor) {
        super(causalityToken);

        this.tableId = tableId;
        this.schemaDescriptor = schemaDescriptor;
    }

    /**
     * Get a table id.
     *
     * @return Table id.
     */
    public UUID tableId() {
        return tableId;
    }

    /**
     * Gets schema descriptor.
     *
     * @return Schema descriptor.
     */
    public SchemaDescriptor schemaDescriptor() {
        return schemaDescriptor;
    }
}
