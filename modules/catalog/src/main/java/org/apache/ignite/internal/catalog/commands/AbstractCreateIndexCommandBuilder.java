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

import java.util.List;

/** Builder that covers attributes which is common among all types of indexes. */
interface AbstractCreateIndexCommandBuilder<T extends AbstractIndexCommandBuilder<T>> extends AbstractIndexCommandBuilder<T> {
    /** Sets a flag indicating whether the {@code IF NOT EXISTS} was specified. */
    T ifNotExists(boolean ifNotExists);

    /** A name of the table an index belongs to. Should not be null or blank. */
    T tableName(String tableName);

    /** A flag denoting whether index keeps at most one row per every key or not. */
    T unique(boolean unique);

    /** List of the columns to index. There must be at least one column. */
    T columns(List<String> columns);
}
