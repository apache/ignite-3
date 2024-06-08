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

import java.util.Objects;
import org.apache.ignite.sql.IgniteSql;

class DropZoneImpl extends AbstractCatalogQuery<Name> {
    private Name zoneName;

    private boolean ifExists;

    DropZoneImpl(IgniteSql sql) {
        super(sql);
    }

    @Override
    protected Name result() {
        return zoneName;
    }

    DropZoneImpl name(String... names) {
        Objects.requireNonNull(names, "Zone name must not be null");

        this.zoneName = new Name(names);
        return this;
    }

    DropZoneImpl ifExists() {
        this.ifExists = true;
        return this;
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.sql("DROP ZONE ");
        if (ifExists) {
            ctx.sql("IF EXISTS ");
        }
        ctx.visit(zoneName);
        ctx.sql(";");
    }
}
