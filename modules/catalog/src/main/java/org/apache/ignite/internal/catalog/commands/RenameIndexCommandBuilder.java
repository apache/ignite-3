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

/** Builder for {@link RenameIndexCommand}s. */
public interface RenameIndexCommandBuilder extends AbstractIndexCommandBuilder<RenameIndexCommandBuilder> {
    /**
     * Sets the new name of the target index.
     *
     * <p>The new name must not be {@code null}, empty or blank. It is also required that an index with the same name does not already exist
     * in the schema that the target index belongs to.
     *
     * @param newIndexName New name of the target index.
     * @return this instance (for chaining).
     */
    RenameIndexCommandBuilder newIndexName(String newIndexName);

    /** Sets a flag indicating whether the {@code IF EXISTS} was specified. */
    RenameIndexCommandBuilder ifIndexExists(boolean ifExists);
}
