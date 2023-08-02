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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.jetbrains.annotations.TestOnly;

/**
 * Schema adapter for apache calcite.
 */
public class IgniteSchema extends AbstractSchema {

    private final String name;

    private final int version;

    private final Map<String, IgniteTable> tableMap;

    /** Constructor. */
    public IgniteSchema(String name, int version, Collection<IgniteTable> tables) {
        this.name = name;
        this.version = version;
        this.tableMap = tables.stream().collect(Collectors.toMap(t -> t.name().toUpperCase(), Function.identity()));
    }

    /** Constructor. */
    @TestOnly
    public IgniteSchema(String name) {
        this(name, 0, List.of());
    }


    /** Schema name. */
    public String getName() {
        return name;
    }

    /** Schema version. */
    public int version() {
        return version;
    }

    /** {@inheritDoc} */
    @Override
    protected Map<String, Table> getTableMap() {
        return Collections.unmodifiableMap(tableMap);
    }

    /** Returns table by given id. */
    public IgniteTable getTable(int tableId) {
        //TODO: optimize this
        return tableMap.values().stream()
                .filter(t -> t.id() == tableId)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No table found: tableId=" + tableId));
    }
}
