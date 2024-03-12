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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.ignite.internal.util.StringUtils;

/**
 * SQL identifier.
 */
class Name extends QueryPart {
    /**
     * Pattern to find possible SQL injections in identifiers.
     */
    private static final Pattern PATTERN = Pattern.compile("[';\r\n\t\\s\\/]|(--[^\r\n]*)");

    private final List<String> names = new ArrayList<>();

    /**
     * Identifier name. E.g [db, schema, table] become 'db.schema.table' or '"db"."schema"."table"' depending on DDL options.
     *
     * @param names name parts of qualified identifier.
     */
    Name(String... names) {
        Objects.requireNonNull(names, "Names must not be null");

        for (String name : names) {
            if (StringUtils.nullOrBlank(name)) {
                // Skip parts instead of failure as nulls can be used as defaults from annotations or builders.
                continue;
            }
            // We need to sanitize the identifiers to prevent SQL injection.
            // Ignite DDL doesn't have prepared statements yet which could be used instead of creating a raw query.
            if (PATTERN.matcher(name).find()) {
                throw new IllegalArgumentException("Name part " + name + " is invalid");
            }
            this.names.add(name);
        }
    }

    @Override
    protected void accept(QueryContext ctx) {
        String quote = ctx.isQuoteNames() ? "\"" : "";
        String separator = "";
        for (String name : names) {
            ctx.sql(separator).sql(quote).sql(name).sql(quote);
            separator = ".";
        }
    }
}
