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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Schema implementation for sql engine.
 */
public class IgniteSchema extends AbstractSchema {

    private final String name;

    private final int catalogVersion;

    private final Map<String, IgniteDataSource> tableByName;

    private final Int2ObjectMap<IgniteDataSource> tableById;

    /** Constructor. */
    public IgniteSchema(String name, int version, Collection<? extends IgniteDataSource> tables) {
        this.name = name;
        this.catalogVersion = version;
        this.tableByName = tables.stream().collect(Collectors.toMap(IgniteDataSource::name, Function.identity()));
        this.tableById = tables.stream().collect(CollectionUtils.toIntMapCollector(IgniteDataSource::id, Function.identity()));
    }

    /** Schema name. */
    public String getName() {
        return name;
    }

    /** Version of the catalog to which this schema correspond. */
    public int catalogVersion() {
        return catalogVersion;
    }

    /** {@inheritDoc} */
    @Override
    protected Map<String, Table> getTableMap() {
        return Collections.unmodifiableMap(tableByName);
    }

    /** Returns table by given id. */
    @Nullable IgniteTable tableByIdOpt(int tableId) {
        IgniteDataSource dataSource = tableById.get(tableId);

        if (!(dataSource instanceof IgniteTable)) {
            return null;
        }

        return (IgniteTable) dataSource;
    }
}
