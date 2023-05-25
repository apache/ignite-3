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
import org.jetbrains.annotations.Nullable;

/**
 * Common interface for {@code ALTER COLUMN} column descriptor change actions.
 */
@FunctionalInterface
public interface ColumnChangeAction {
    /**
     * Column descriptor change order.
     *
     * <p>Column descriptor changes must be applied in a specific order.
     * For example, for the following command, {@code DROP DEFAULT, DROP NOT NULL}, column descriptor must be marked nullable before
     * attempting to drop the default value.
     */
    public enum Priority {
        DATA_TYPE(0),
        DROP_NOT_NULL(1),
        DEFAULT(2),
        SET_NOT_NULL(3),
        UNDEFINED(Integer.MAX_VALUE);

        private final int priority;

        Priority(int priority) {
            this.priority = priority;
        }

        public int value() {
            return priority;
        }
    }

    /**
     * Changes column descriptor.
     *
     * @param desc Column descriptor.
     * @return Modified column descriptor, or {@code null} if no changes were made.
     */
    @Nullable TableColumnDescriptor apply(TableColumnDescriptor desc);

    /** Gets the order priority for the change action. */
    default Priority priority() {
        return Priority.UNDEFINED;
    }
}
