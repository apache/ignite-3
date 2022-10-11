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

package org.apache.ignite.internal.storage.index;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.schema.NativeType;

/**
 * Index descriptor.
 */
public interface IndexDescriptor {
    /**
     * Index column descriptor.
     */
    interface ColumnDescriptor {
        /**
         * Returns the name of an index column.
         */
        String name();

        /**
         * Returns a column type.
         */
        NativeType type();

        /**
         * Returns {@code true} if this column can contain null values or {@code false} otherwise.
         */
        boolean nullable();
    }

    /**
     * Returns the index ID.
     */
    UUID id();

    /**
     * Returns index column descriptions.
     */
    List<? extends ColumnDescriptor> columns();
}
