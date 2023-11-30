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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlMerge;
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
        CollectTableNames collectTableNames = new CollectTableNames();

        rootNode.accept(collectTableNames);

        return collectTableNames.tableIds.stream()
                .map(sqlId -> Util.last(sqlId.names))
                .collect(toSet());
    }

    private static class CollectTableNames extends SqlBasicVisitor<Void> {
        private final Set<SqlIdentifier> tableIds = new HashSet<>();

        private boolean collect;

        @Override
        public Void visit(SqlIdentifier identifier) {
            if (collect) {
                tableIds.add(identifier);
            }

            return null;
        }

        @Override
        public Void visit(SqlCall call) {
            switch (call.getKind()) {
                case SELECT:
                    return visitSelect((SqlSelect) call);
                case JOIN:
                    return visitJoin((SqlJoin) call);
                case AS:
                    return visitAs((SqlBasicCall) call);
                case INSERT:
                    return visitInsert((SqlInsert) call);
                case UPDATE:
                    return visitUpdate((SqlUpdate) call);
                case DELETE:
                    return visitDelete((SqlDelete) call);
                case MERGE:
                    return visitMerge((SqlMerge) call);
                default:
                    return super.visit(call);
            }
        }

        private Void visitSelect(SqlSelect select) {
            boolean oldCollect = collect;
            collect = false;

            for (SqlNode child : select.getOperandList()) {
                if (child == null) {
                    continue;
                }

                collect = select.getFrom() == child;

                child.accept(this);
            }

            collect = oldCollect;

            return null;
        }

        private Void visitJoin(SqlJoin join) {
            for (SqlNode child : join.getOperandList()) {
                if (child == null) {
                    continue;
                }

                if (child == join.getCondition()) {
                    boolean oldCollect = collect;
                    collect = false;

                    child.accept(this);

                    collect = oldCollect;
                } else {
                    child.accept(this);
                }
            }

            return null;
        }

        private Void visitAs(SqlBasicCall as) {
            int childIndex = 0;

            for (SqlNode child : as.getOperandList()) {
                if (child != null) {
                    if (childIndex == 1) {
                        boolean oldCollect = collect;
                        collect = false;

                        child.accept(this);

                        collect = oldCollect;
                    } else {
                        child.accept(this);
                    }
                }

                childIndex++;
            }

            return null;
        }

        private Void visitInsert(SqlInsert insert) {
            boolean isInFromBackup = collect;

            for (SqlNode child : insert.getOperandList()) {
                if (child == null) {
                    continue;
                }

                collect = insert.getTargetTable() == child;

                child.accept(this);
            }

            collect = isInFromBackup;

            return null;
        }

        private Void visitUpdate(SqlUpdate update) {
            boolean oldCollect = collect;

            for (SqlNode child : update.getOperandList()) {
                if (child == null) {
                    continue;
                }

                collect = update.getTargetTable() == child;

                child.accept(this);
            }

            collect = oldCollect;

            return null;
        }

        private Void visitDelete(SqlDelete delete) {
            boolean oldCollect = collect;

            for (SqlNode child : delete.getOperandList()) {
                if (child == null) {
                    continue;
                }

                collect = delete.getTargetTable() == child;

                child.accept(this);
            }

            collect = oldCollect;

            return null;
        }

        private Void visitMerge(SqlMerge merge) {
            boolean oldCollect = collect;

            for (SqlNode child : merge.getOperandList()) {
                if (child == null) {
                    continue;
                }

                collect = child == merge.getTargetTable()
                        || child == merge.getSourceTableRef();

                child.accept(this);
            }

            collect = oldCollect;

            return null;
        }
    }
}
