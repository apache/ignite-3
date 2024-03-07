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

class Column extends QueryPart {
    private final Name name;

    private final ColumnTypeImpl<?> type;

    private final String definition;

    Column(String name, ColumnTypeImpl<?> type) {
        this.name = new Name(name);
        this.type = type;
        this.definition = null;
    }

    Column(String name, String definition) {
        this.name = new Name(name);
        this.type = null;
        this.definition = definition;
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.visit(name).sql(" ");
        if (type != null) {
            ctx.visit(type);
        } else if (definition != null) {
            ctx.sql(definition);
        }
    }
}
