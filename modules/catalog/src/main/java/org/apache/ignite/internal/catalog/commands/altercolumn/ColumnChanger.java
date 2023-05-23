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

import java.util.function.Function;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;

public interface ColumnChanger extends Function<TableColumnDescriptor, TableColumnDescriptor> {
    public enum Priority {
        DATA_TYPE(0),
        DROP_NOT_NULL(1),
        DEFAULT(2),
        SET_NOT_NULL(3),
        IGNORE(Integer.MAX_VALUE);

        private final int priority;

        Priority(int priority) {
            this.priority = priority;
        }

        public int order() {
            return priority;
        }
    }

    default Priority priority() {
        return Priority.IGNORE;
    }
}
