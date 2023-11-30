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

package org.apache.ignite.internal.sql.engine;

import static java.util.stream.Collectors.toSet;

import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Util;

/**
 * Utils to work with {@link SqlNode}s.
 */
class SqlNodeUtils {
    /**
     * Extracts simple names of all tables mentioned in a query represented by the node.
     *
     * @param rootNode Node representing a query.
     */
    static Set<String> extractTableNames(SqlNode rootNode) {
        // TODO: IGNITE-20107 - real implementation.

        if (rootNode instanceof SqlSelect) {
            SqlNode from = ((SqlSelect) rootNode).getFrom();

            if (from != null) {
                return extractIdentifiers(from);
            }
        } else if (rootNode instanceof SqlUpdate) {
            SqlNode targetTable = ((SqlUpdate) rootNode).getTargetTable();

            if (targetTable != null) {
                return extractIdentifiers(targetTable);
            }
        }

        return Set.of();
    }

    private static Set<String> extractIdentifiers(SqlNode from) {
        CollectIdentifiers collectIdentifiers = new CollectIdentifiers();
        from.accept(collectIdentifiers);

        return collectIdentifiers.ids.stream()
                .map(sqlId -> Util.last(sqlId.names))
                .collect(toSet());
    }

    private static class CollectIdentifiers extends SqlBasicVisitor<Void> {
        private final Set<SqlIdentifier> ids = new HashSet<>();

        @Override
        public Void visit(SqlIdentifier identifier) {
            ids.add(identifier);

            return null;
        }
    }
}
