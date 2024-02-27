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

import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;

/** {@link BinaryRow} upgrader to the required schema version. */
public class BinaryRowUpgrader {
    private final SchemaRegistry schemaRegistry;

    private final SchemaDescriptor targetSchema;

    /** Constructor. */
    public BinaryRowUpgrader(SchemaRegistry schemaRegistry, int targetSchemaVersion) {
        this(schemaRegistry, schemaRegistry.schema(targetSchemaVersion));
    }

    /** Constructor. */
    public BinaryRowUpgrader(SchemaRegistry schemaRegistry, SchemaDescriptor targetSchema) {
        this.schemaRegistry = schemaRegistry;
        this.targetSchema = targetSchema;
    }

    /**
     * Returns an upgraded {@link BinaryRow} to the required schema version, or the {@code source} if its schema version is greater than or
     * equal to to the required schema version.
     *
     * @param source Source binary row.
     */
    public BinaryRow upgrade(BinaryRow source) {
        if (source.schemaVersion() >= targetSchema.version()) {
            return source;
        }

        Row upgradedRow = schemaRegistry.resolve(source, targetSchema);

        var rowAssembler = new RowAssembler(targetSchema, -1);

        for (int i = 0; i < targetSchema.length(); i++) {
            rowAssembler.appendValue(upgradedRow.value(i));
        }

        return new BinaryRowImpl(targetSchema.version(), rowAssembler.build().tupleSlice());
    }
}
