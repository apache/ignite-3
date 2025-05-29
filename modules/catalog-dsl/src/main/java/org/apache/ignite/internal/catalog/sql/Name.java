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

import java.util.List;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.table.QualifiedName;

/**
 * SQL identifier.
 */
class Name extends QueryPart {
    private final List<String> names;

    private final QualifiedName qualifiedName;

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
     * Creates a name from a qualified name.
     *
     * @param qualifiedName Qualified name.
     * @return Name.
     */
    static Name qualified(QualifiedName qualifiedName) {
        return new Name(qualifiedName);
    }

    private Name(QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
        this.names = null;
    }

    private Name(List<String> names) {
        for (String name : names) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Name parts can not be null or empty: " + names);
            }
        }
        this.qualifiedName = null;
        this.names = names;
    }

    @Override
    protected void accept(QueryContext ctx) {
        if (qualifiedName != null) {
            ctx.sql(qualifiedName.toCanonicalForm());
        } else {
            assert names != null : "Names must be specified";

            String separator = "";
            for (String name : names) {
                // If a name is quoted, we must preserve case sensitivity -> write it as is
                // If a name UPPER(name) is a valid normalized id, then this is a case insensitive name,
                // write it in uppercase for consistency. Otherwise we must quote it.
                if (name.startsWith("\"")) {
                    ctx.sql(separator).sql(name);
                } else {
                    String upperCase = name.toUpperCase();
                    if (IgniteNameUtils.isValidNormalizedIdentifier(upperCase)) {
                        ctx.sql(separator).sql(upperCase);
                    } else {
                        ctx.sql(separator).sql(IgniteNameUtils.quoteIfNeeded(name));
                    }
                }
                separator = ".";
            }
        }
    }
}
