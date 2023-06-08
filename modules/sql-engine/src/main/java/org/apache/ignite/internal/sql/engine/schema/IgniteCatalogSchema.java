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

import java.util.Collections;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * Schema adapter for apache calcite.
 */
public class IgniteCatalogSchema extends AbstractSchema {

    private final String name;

    private final int version;

    private final Map<String, Table> tableMap;

    /** Constructor. */
    public IgniteCatalogSchema(String name, int version, Map<String, Table> tableMap) {
        this.name = name;
        this.version = version;
        this.tableMap = tableMap;
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
}
