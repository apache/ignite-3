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
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.lang.util.IgniteNameUtils;

/**
 * SQL identifier.
 */
class Name extends QueryPart {
    private final List<String> names;

    /**
     * Creates a simple name.
     *
     * @param name Name.
     * @return Name.
     */
    static Name simple(String name) {
        return new Name(List.of(name));
    }

    /**
     * Creates a compound name (e.g. {@code my_schema.my_table} or {@code my_table}). Accepts both simple and compound names.
     *
     * @param names Array of names.
     * @return Name.
     */
    static Name compound(String... names) {
        List<String> parts = new ArrayList<>(2);

        if (names.length == 0) {
            throw new IllegalArgumentException("Names can not be empty");
        } else if (names.length == 1) {
            return new Name(Arrays.asList(names));
        } else {
            boolean canSkipEmpty = true;
            for (String part : names) {
                if (part == null || part.isEmpty()) {
                    if (canSkipEmpty) {
                        canSkipEmpty = false;
                        continue;
                    }
                } else {
                    // Do not allow non-leading elements to be empty
                    canSkipEmpty = false;
                }
                parts.add(part);
            }

            return new Name(parts);
        }
    }

    private Name(List<String> names) {
        for (String name : names) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Name parts can not be null or empty: " + names);
            }
        }
        this.names = names;
    }

    @Override
    protected void accept(QueryContext ctx) {
        String separator = "";
        for (String name : names) {
            ctx.sql(separator).sql(IgniteNameUtils.parseAndNormalize(name));
            separator = ".";
        }
    }
}
