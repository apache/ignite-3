/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.exec.StatementMismatchException;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan.Type;

/**
 * The validator interface for checking actual query type against the expected query type.
 * Allows to validate both an existing plan and a newly parsed query.
 * */
public interface QueryValidator {
    /**
     * Checks if the expected query type is QUERY (SELECT).
     *
     * @return {@code true} if expected type is QUERY, {@code false} otherwise.
     */
    boolean isQuery();

    /**
     * Checks the prepared query plan type against the expected query type.
     *
     * @throws StatementMismatchException in the case of a validation error.
     */
    default void validatePlan(QueryPlan plan) throws StatementMismatchException {
        if (plan.type() == Type.QUERY || plan.type() == Type.EXPLAIN) {
            if (isQuery()) {
                return;
            }
            throw new StatementMismatchException();
        }
        if (!isQuery()) {
            return;
        }
        throw new StatementMismatchException();
    }

    /**
     * Checks the parsed query type against the expected query type.
     *
     * @throws StatementMismatchException in the case of a validation error.
     */
    default void validateParsedQuery(SqlNode rootNode) throws StatementMismatchException {
        if (SqlKind.QUERY.contains(rootNode.getKind())) {
            if (isQuery()) {
                return;
            }
            throw new StatementMismatchException();
        }
        if (!isQuery()) {
            return;
        }
        throw new StatementMismatchException();
    }
}
