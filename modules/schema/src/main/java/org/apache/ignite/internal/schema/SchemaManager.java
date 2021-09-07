/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.configuration.SchemaDescriptorConverter;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.schema.SchemaTable;

// TODO sanpwc: rework javadoc.
/**
 * Schema Manager.
 * <p>
 * Schemas MUST be registered in a version ascending order incrementing by {@code 1} with NO gaps,
 * otherwise an exception will be thrown. The version numbering starts from the {@code 1}.
 * <p>
 * After some table maintenance process some first versions may become outdated and can be safely cleaned up
 * if the process guarantees the table no longer has a data of these versions.
 *
 * @implSpec The changes in between two arbitrary actual versions MUST NOT be lost.
 * Thus, schema versions can only be removed from the beginning.
 * @implSpec Initial schema history MAY be registered without the first outdated versions
 * that could be cleaned up earlier.
 */
public class SchemaManager extends Producer<SchemaEvent, SchemaEventParameters> implements IgniteComponent {
    /** {@inheritDoc} */
    @Override public void start() {
        // No-op. Schema manager is stateless helper manager.
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op. Schema manager is stateless nothing to stop.
    }

    /**
     * Creates schema descriptor for the table with specified configuration.
     *
     * @param tblCfg Table configuration.
     * @return Schema descriptor.
     */
    public SchemaDescriptor prepareSchemaDescriptor(TableView tblCfg) {
        SchemaTable schemaTbl = SchemaConfigurationConverter.convert(tblCfg);

        // TODO sanpwc: Not sure whether it should be 1.
        // TODO sanpwc: Proper calculation of schema is required.
        // TODO sanpwc: consider removing schmea vesion from schema object.
        return SchemaDescriptorConverter.convert(1, schemaTbl);
    }

    /**
     * Compares schemas.
     *
     * @param expected Expected schema.
     * @param actual Actual schema.
     * @return {@code True} if schemas are equal, {@code false} otherwise.
     */
    public static boolean equalSchemas(SchemaDescriptor expected, SchemaDescriptor actual) {
        if (expected.keyColumns().length() != actual.keyColumns().length() ||
            expected.valueColumns().length() != actual.valueColumns().length())
            return false;

        for (int i = 0; i < expected.length(); i++) {
            if (!expected.column(i).equals(actual.column(i)))
                return false;
        }

        return true;
    }
}
