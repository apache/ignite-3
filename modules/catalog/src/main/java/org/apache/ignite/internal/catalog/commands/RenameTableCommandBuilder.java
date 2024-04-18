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

package org.apache.ignite.internal.catalog.commands;

/** Builder for {@link RenameTableCommand}s. */
public interface RenameTableCommandBuilder extends AbstractTableCommandBuilder<RenameTableCommandBuilder> {
    /**
     * Sets the new name of the target table.
     *
     * <p>The new name must not be {@code null}, empty or blank. It is also required that a table with the same name does not already exist
     * in the schema that the target table belongs to.
     *
     * @param newTableName New name of the target table.
     * @return this instance (for chaining).
     */
    RenameTableCommandBuilder newTableName(String newTableName);
}
