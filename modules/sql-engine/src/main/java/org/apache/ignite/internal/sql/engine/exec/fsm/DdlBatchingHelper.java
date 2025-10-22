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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

/**
 *  Provide helper methods for batched DDL commands.
 */
class DdlBatchingHelper {
    /**
     * Return {@code true} if a DDL command is compatible with batched DDL commands and can be added to the batch.
     *
     * @param batch DDL commands represented by AST trees.
     * @param statement A DDL statement represented by AST tree
     * @return {@code true} if statement can be added to the batch, {@code false} otherwise.
     */
    static boolean canBeBatched(List<SqlNode> batch, SqlNode statement) {
        return batch.stream().allMatch(s -> isCompatible(s, statement));
    }

    /** Returns {@code true} if commands (represented by AST trees) can be executed together, {@code false} otherwise. */
    private static boolean isCompatible(SqlNode node1, SqlNode node2) {
        DdlCommandType kind1 = getCommandType(node1);
        DdlCommandType kind2 = getCommandType(node2);

        return kind1 == kind2 && kind1 != DdlCommandType.OTHER;
    }

    /** Returns command kind. */
    private static DdlCommandType getCommandType(SqlNode node) {
        SqlKind kind = node.getKind();

        switch (kind) {
            case CREATE_SCHEMA:
            case CREATE_TABLE:
            case CREATE_SEQUENCE:
            case CREATE_INDEX:
                return DdlCommandType.CREATE;

            case DROP_SCHEMA:
            case DROP_TABLE:
            case DROP_INDEX:
            case DROP_SEQUENCE:
                return DdlCommandType.DROP;

            case OTHER_DDL:
            default:
                return DdlCommandType.OTHER;
        }
    }

    /** Describes DDL command kind. */
    private enum DdlCommandType {
        /** Create command. */
        CREATE,
        /** Drop command. */
        DROP,
        /** Any other DDL command (ALTER command, zone commands, and others) */
        OTHER
    }
}
