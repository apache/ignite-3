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

package org.apache.ignite.internal.catalog.sql;

import org.apache.ignite.catalog.Options;
import org.apache.ignite.catalog.Query;
import org.apache.ignite.sql.IgniteSql;

abstract class AbstractCatalogQuery extends QueryPart implements Query {
    protected final IgniteSql sql;

    protected final Options options;

    AbstractCatalogQuery(IgniteSql sql, Options options) {
        this.sql = sql;
        this.options = options;
    }

    @Override
    public void execute() {
        sql.executeScript(toSqlString());
    }

    @Override
    public String toSqlString() {
        return new QueryContext(options)
                .visit(this)
                .getSql();
    }

    @Override
    public String toString() {
        return new QueryContext(Options.builder().prettyPrint().build())
                .visit(this)
                .getSql();
    }
}
