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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.BitSet;
import org.jetbrains.annotations.Nullable;

/**
 * A factory to create a converter from table row to execution engine representation
 * with regard to the set of required columns.
 */
@FunctionalInterface
public interface TableRowConverterFactory {
    TableRowConverter create(@Nullable BitSet requiredColumns);

    default TableRowConverter create(BitSet requiredColumns, int partId) {
        return create(requiredColumns);
    }
}
