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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Possible query types.
 */
public enum SqlQueryType {
    /** Query. */
    QUERY,

    /** DML. */
    DML,

    /** DDL. */
    DDL,

    /** Explain. */
    EXPLAIN,

    /** Transaction control statements such as {@code START TRANSACTION}, {@code COMMIT},  etc. */
    TX_CONTROL,

    /** Kill statement (such as {@code KILL QUERY <ID>}, {@code KILL TRANSACTION <ID>}, etc.  */
    KILL,

    ;

    /** A set of statement types that can run only in single statement mode. **/
    public static final Set<SqlQueryType> SINGLE_STMT_TYPES = EnumSet.copyOf(Arrays.stream(values())
            .filter(SqlQueryType::supportsIndependentExecution)
            .collect(Collectors.toList()));

    /** A set of all query types. **/
    public static final Set<SqlQueryType> ALL = EnumSet.allOf(SqlQueryType.class);

    /** A set of types that {@link #hasRowSet() has row set}. Usually represented by SELECT statement. */
    public static final Set<SqlQueryType> HAS_ROW_SET_TYPES = Arrays.stream(values())
            .filter(SqlQueryType::hasRowSet).collect(Collectors.toCollection(() -> EnumSet.noneOf(SqlQueryType.class)));

    /** A set of types that {@link #returnsAffectedRows() returns number of affected rows}. Represented by various DML statements. */
    public static final Set<SqlQueryType> RETURNS_AFFECTED_ROWS_TYPES = Arrays.stream(values())
            .filter(SqlQueryType::returnsAffectedRows).collect(Collectors.toCollection(() -> EnumSet.noneOf(SqlQueryType.class)));

    /**
     * A set of types that {@link #supportsWasApplied() returns boolean indicating whether command was applied or not}. Represented by
     * various DDL statements and operational commands.
     */
    public static final Set<SqlQueryType> SUPPORT_WAS_APPLIED_TYPES = Arrays.stream(values())
            .filter(SqlQueryType::supportsWasApplied).collect(Collectors.toCollection(() -> EnumSet.noneOf(SqlQueryType.class)));

    /**
     * Returns {@code true} if a parse tree of a statement of this type should be cached.
     * Otherwise returns {@code false}.
     */
    public boolean supportsParseResultCaching() {
        switch (this) {
            case QUERY:
            case DML:
                return true;
            case DDL:
            case EXPLAIN:
            case TX_CONTROL:
            case KILL:
                return false;
            default:
                throw unexpectedSqlQueryType(this);
        }
    }

    /**
     * Returns {@code true} if a statement of this type support EXPLAIN PLAN.
     * Otherwise returns {@code false}.
     */
    public boolean supportsExplain() {
        switch (this) {
            case QUERY:
            case DML:
                return true;
            case DDL:
            case EXPLAIN:
            case TX_CONTROL:
            case KILL:
                return false;
            default:
                throw unexpectedSqlQueryType(this);
        }
    }

    /**
     * Returns {@code true} if a statement of this type can be executed as a standalone statement.
     * Otherwise returns {@code false}.
     */
    public boolean supportsIndependentExecution() {
        switch (this) {
            case QUERY:
            case DDL:
            case DML:
            case EXPLAIN:
            case KILL:
                return true;
            case TX_CONTROL:
                return false;
            default:
                throw unexpectedSqlQueryType(this);
        }
    }

    /**
     * Returns {@code true} if a statement of this type support transactions (e.g. can be committed or rollbacked).
     * Otherwise returns {@code false}.
     */
    public boolean supportsTransactions() {
        switch (this) {
            case QUERY:
            case DML:
                return true;
            case TX_CONTROL:
            case DDL:
            case EXPLAIN:
            case KILL:
                return false;
            default:
                throw unexpectedSqlQueryType(this);
        }
    }

    /**
     * Returns {@code true} if a statement of this type supports running inside an explicit transaction.
     * Otherwise returns {@code false}.
     */
    public boolean supportsExplicitTransactions() {
        switch (this) {
            case QUERY:
            case DML:
            case KILL:
            case EXPLAIN:
                return true;
            case DDL:
            case TX_CONTROL:
                return false;
            default:
                throw unexpectedSqlQueryType(this);
        }
    }

    /**
     * Read-only mode used for implicit transaction. If this method returns {@code true}, then implicit transaction
     * is started in read-only mode, otherwise implicit transaction is started in read-write mode.
     *
     * @throws UnsupportedOperationException If a statement of this type does not support implicit transactions.
     */
    public boolean implicitTransactionReadOnlyMode() {
        switch (this) {
            case QUERY:
            case EXPLAIN:
                return true;
            case DML:
                return false;
            case DDL:
            case TX_CONTROL:
            case KILL:
                throw new UnsupportedOperationException(this + " does not support requesting transaction mode.");
            default:
                throw unexpectedSqlQueryType(this);
        }
    }

    /**
     * Returns {@code true} if a statement of this type produces a row set.
     */
    public boolean hasRowSet() {
        switch (this) {
            case QUERY:
            case EXPLAIN:
                return true;
            case DML:
            case DDL:
            case TX_CONTROL:
            case KILL:
                return false;
            default:
                throw unexpectedSqlQueryType(this);
        }
    }

    /**
     * Returns {@code true} if a statement of this type produces the number of affected rows.
     */
    public boolean returnsAffectedRows() {
        switch (this) {
            case QUERY:
            case DDL:
            case EXPLAIN:
            case TX_CONTROL:
            case KILL:
                return false;
            case DML:
                return true;
            default:
                throw unexpectedSqlQueryType(this);
        }
    }

    /**
     * Returns {@code true} if a result set for a statement of this type can use {@code wasApplied} flag.
     */
    public boolean supportsWasApplied() {
        switch (this) {
            case QUERY:
            case DML:
            case EXPLAIN:
            case TX_CONTROL:
                return false;
            case DDL:
            case KILL:
                return true;
            default:
                throw unexpectedSqlQueryType(this);
        }
    }

    /** Returns a human readable name of this statement type. */
    public String displayName() {
        switch (this) {
            case QUERY:
                return "Query";
            case DML:
                return "DML";
            case DDL:
                return "DDL";
            case EXPLAIN:
                return "Explain";
            case TX_CONTROL:
                return "Transaction control statement";
            case KILL:
                return "Kill";
            default:
                throw unexpectedSqlQueryType(this);
        }
    }

    private static IllegalStateException unexpectedSqlQueryType(SqlQueryType type) {
        return new IllegalStateException("Unexpected query type:" + type);
    }
}
