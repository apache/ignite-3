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

package org.apache.ignite.migrationtools.tablemanagement;

import org.jetbrains.annotations.Nullable;

/** This interface provides a registry for mappings between tables and their corresponding java types. */
public interface TableTypeRegistry {
    /**
     * Gets the available type hints for the table.
     *
     * @param tableName Must be escaped according to Ignite 3 rules.
     * @return The type hints for the table, or null if none are available.
     */
    @Nullable TableTypeDescriptor typesForTable(String tableName);

    /**
     * Registers the supplied type hints for the given table. Existing hints will be replaced.
     *
     * @param tableName Must be escaped according to Ignite 3 rules.
     * @param tableDescriptor Table Descriptor.
     */
    void registerTypesForTable(String tableName, TableTypeDescriptor tableDescriptor);
}
