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

import org.apache.ignite.sql.ColumnType;

/**
 * A default value provider, whose value depends on actual type of the column this value specified for.

 * For operation that updates a certain column in a table, type of the column
 * is not know until command will be applied to the actual version of catalog.
 * Therefore, we need to provide a way to defer calculation of a new default
 * until all necessary information is available.
 */
@FunctionalInterface
public interface DeferredDefaultValue {
    DefaultValue derive(ColumnType type);
}
