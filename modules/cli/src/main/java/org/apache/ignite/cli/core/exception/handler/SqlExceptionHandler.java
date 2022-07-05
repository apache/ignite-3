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

package org.apache.ignite.cli.core.exception.handler;

import java.sql.SQLException;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Exception handler for {@link SQLException}.
 */
public class SqlExceptionHandler implements ExceptionHandler<SQLException> {
    private static final IgniteLogger log = Loggers.forClass(SqlExceptionHandler.class);

    public static final String PARSING_ERROR_MESSAGE = "SQL query parsing error: %s";

    public static final String INVALID_PARAMETER_MESSAGE = "Invalid parameter value.";

    public static final String CLIENT_CONNECTION_FAILED_MESSAGE = "Connection failed.";

    public static final String CONNECTION_BROKE_MESSAGE = "Connection error.";

    @Override
    public int handle(ExceptionWriter err, SQLException e) {
        switch (e.getSQLState()) {
            case SqlStateCode.CONNECTION_FAILURE:
            case SqlStateCode.CONNECTION_CLOSED:
            case SqlStateCode.CONNECTION_REJECTED:
                err.write(CONNECTION_BROKE_MESSAGE);
                break;
            case SqlStateCode.PARSING_EXCEPTION:
                err.write(String.format(PARSING_ERROR_MESSAGE, e.getMessage()));
                break;
            case SqlStateCode.INVALID_PARAMETER_VALUE:
                err.write(INVALID_PARAMETER_MESSAGE);
                break;
            case SqlStateCode.CLIENT_CONNECTION_FAILED:
                err.write(CLIENT_CONNECTION_FAILED_MESSAGE);
                break;
            default:
                log.error("Unrecognized error ", e);
                err.write("Unrecognized error while process SQL query.");
        }
        return 1;
    }

    @Override
    public Class<SQLException> applicableException() {
        return SQLException.class;
    }
}
