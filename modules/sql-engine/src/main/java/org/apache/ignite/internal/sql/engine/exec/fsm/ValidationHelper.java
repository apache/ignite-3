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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.Set;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.sql.SqlException;

/**
 * Helper class to share validation amongst different phase handlers as well as outside of the package.
 */
public final class ValidationHelper {
    /** Performs additional validation of a parsed statement. **/
    public static void validateParsedStatement(
            SqlProperties properties,
            ParsedResult parsedResult
    ) {
        Set<SqlQueryType> allowedTypes = properties.allowedQueryTypes();
        SqlQueryType queryType = parsedResult.queryType();

        if (!parsedResult.queryType().supportsIndependentExecution()) {
            String message = queryType.displayName() + " can not be executed as an independent statement.";

            throw new SqlException(STMT_VALIDATION_ERR, message);
        }

        validateQueryType(allowedTypes, queryType);
    }

    /** Performs validation of dynamic params provided. **/
    public static void validateDynamicParameters(int expectedParamsCount, Object[] params, boolean exactMatch) throws SqlException {
        if (exactMatch && expectedParamsCount != params.length || params.length > expectedParamsCount) {
            String message = format(
                    "Unexpected number of query parameters. Provided {} but there is only {} dynamic parameter(s).",
                    params.length, expectedParamsCount
            );

            throw new SqlException(STMT_VALIDATION_ERR, message);
        }

        for (Object param : params) {
            if (!TypeUtils.supportParamInstance(param)) {
                String message = format(
                        "Unsupported dynamic parameter defined. Provided '{}' is not supported.", param.getClass().getName());

                throw new SqlException(STMT_VALIDATION_ERR, message);
            }
        }
    }

    /** Validates query type. */
    static void validateQueryType(
            Set<SqlQueryType> allowedTypes,
            SqlQueryType queryType
    ) {
        if (!allowedTypes.contains(queryType)) {
            String message = format("Invalid SQL statement type. Expected {} but got {}.", allowedTypes, queryType);

            throw new SqlException(STMT_VALIDATION_ERR, message);
        }
    }
}
