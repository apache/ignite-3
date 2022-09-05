/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.schema.definition;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.schema.definition.index.IndexDefinition;

/**
 * Table schema configuration.
 */
public interface TableDefinition extends SchemaObject {

    /**
     * Returns table name.
     *
     * @return Table name.
     */
    @Override
    String name();

    /**
     * Returns key columns.
     *
     * @return List of columns.
     */
    Set<String> keyColumns();

    /**
     * Returns affinity columns.
     *
     * @return List of columns.
     */
    List<String> colocationColumns();

    /**
     * Returns table columns in user-defined order.
     *
     * @return List of columns.
     */
    List<ColumnDefinition> columns();

    /**
     * Returns table indices.
     *
     * @return Collection of indexes.
     */
    Collection<IndexDefinition> indices();
}
