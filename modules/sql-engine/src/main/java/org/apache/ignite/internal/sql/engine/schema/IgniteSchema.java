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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.NameMap;
import org.jetbrains.annotations.TestOnly;

/**
 * Schema implementation for sql engine.
 */
public class IgniteSchema extends AbstractSchema {

    private final String name;

    private final int catalogVersion;

    private final Lookup<Table> tableLookup;

    /** Constructor. */
    @TestOnly
    public IgniteSchema(String name, int catalogVersion, Collection<? extends IgniteDataSource> tables) {
        this(
                name,
                catalogVersion,
                Lookup.of(NameMap.immutableCopyOf(tables.stream().collect(Collectors.toMap(IgniteDataSource::name, Table.class::cast))))
        );
    }

    /** Constructor. */
    public IgniteSchema(String name, int catalogVersion, Lookup<Table> tableLookup) {
        this.name = name;
        this.catalogVersion = catalogVersion;
        this.tableLookup = tableLookup;
    }

    @Override
    public Lookup<Table> tables() {
        return tableLookup;
    }

    /** Schema name. */
    public String getName() {
        return name;
    }

    /** Version of the catalog to which this schema correspond. */
    public int catalogVersion() {
        return catalogVersion;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        throw new UnsupportedOperationException();
    }
}
