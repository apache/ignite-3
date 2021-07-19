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

import org.apache.ignite.internal.schema.registry.SchemaRegistryException;
import org.apache.ignite.internal.schema.row.Row;
import org.jetbrains.annotations.NotNull;

/**
 * Table schema registry interface.
 */
public interface SchemaRegistry {
    /**
     * Gets schema descriptor for the latest version if initialized.
     *
     * @return Schema descriptor if initialized, {@code null} otherwise.
     */
    SchemaDescriptor schema();

    /**
     * @return Last registereg schema version.
     */
    public int lastSchemaVersion();

    /**
     * Gets schema descriptor for given version.
     *
     * @param ver Schema version to get descriptor for.
     * @return Schema descriptor of given version.
     * @throws SchemaRegistryException If no schema found for given version.
     */
    @NotNull SchemaDescriptor schema(int ver) throws SchemaRegistryException;

    /**
     * @param row Binary row.
     * @return Schema-aware row.
     */
    Row resolve(BinaryRow row);
}
