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

package org.apache.ignite.internal.catalog.commands.altercolumn;

import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;

/**
 * Common interface for {@code ALTER COLUMN} column descriptor change actions.
 */
@FunctionalInterface
public interface AlterColumnAction {
    /**
     * Changes column descriptor.
     *
     * @param origin Column descriptor.
     * @param isPkColumn {@code True} if this column is part of the primary key, {@code false} otherwise.
     * @return Modified copy of the {@code origin} column descriptor, or {@code origin} descriptor if no changes were made.
     */
    TableColumnDescriptor apply(TableColumnDescriptor origin, boolean isPkColumn);
}
