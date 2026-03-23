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

package org.apache.ignite.internal.cli.core.exception.handler;

import static org.apache.ignite.lang.ErrorGroups.extractCauseMessage;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import javax.net.ssl.SSLHandshakeException;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.style.component.ErrorUiComponent;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.jetbrains.annotations.Nullable;

/**
 * Exception handler for {@link SQLException}.
 */
public class SqlExceptionHandler implements ExceptionHandler<SQLException> {
    public static final SqlExceptionHandler INSTANCE = new SqlExceptionHandler();

    private static final IgniteLogger LOG = Loggers.forClass(SqlExceptionHandler.class);

    private static final String CLIENT_CONNECTION_FAILED_MESSAGE = "Connection failed";

    private static final String UNRECOGNIZED_ERROR_MESSAGE = "Unrecognized error while processing SQL query ";

    private final Map<Short, Function<IgniteException, ErrorUiComponent>> sqlExceptionMappers = new HashMap<>();

    /** Default constructor. */
    private SqlExceptionHandler() {
        sqlExceptionMappers.put(Client.CLIENT_ERR_GROUP.groupCode(), SqlExceptionHandler::connectionErrUiComponent);
        sqlExceptionMappers.put(Sql.SQL_ERR_GROUP.groupCode(), SqlExceptionHandler::sqlErrUiComponent);
    }

    private static ErrorUiComponent connectionErrUiComponent(IgniteException e) {
        if (e.code() != Client.CONNECTION_ERR) {
            return fromIgniteException("Client error", e);
        }

        if (e.getCause() instanceof IgniteClientConnectionException) {
            IgniteClientConnectionException cause = (IgniteClientConnectionException) e.getCause();

            InvalidCredentialsException invalidCredentialsException = findCause(cause, InvalidCredentialsException.class);
            if (invalidCredentialsException != null) {
                return ErrorUiComponent.builder()
                        .header("Could not connect to node. Check authentication configuration")
                        .details(invalidCredentialsException.getMessage())
                        .verbose(extractCauseMessage(cause.getMessage()))
                        .build();
            }

            SSLHandshakeException sslHandshakeException = findCause(cause, SSLHandshakeException.class);
            if (sslHandshakeException != null) {
                return ErrorUiComponent.builder()
                        .header("Could not connect to node. Check SSL configuration")
                        .details(sslHandshakeException.getMessage())
                        .verbose(extractCauseMessage(cause.getMessage()))
                        .build();
            }

            return fromIgniteException(CLIENT_CONNECTION_FAILED_MESSAGE, cause);
        }

        return fromIgniteException(CLIENT_CONNECTION_FAILED_MESSAGE, e);
    }

    @Nullable
    private static <T extends Throwable> T findCause(Throwable e, Class<T> type) {
        while (e != null) {
            if (type.isInstance(e)) {
                return (T) e;
            }
            e = e.getCause();
        }
        return null;
    }

    @Override
    public int handle(ExceptionWriter err, SQLException e) {
        Throwable unwrappedCause = ExceptionUtils.unwrapCause(e.getCause());
        ErrorUiComponent errorComponent;

        if (unwrappedCause instanceof IgniteException) {
            IgniteException igniteException = (IgniteException) unwrappedCause;

            errorComponent = sqlExceptionMappers.getOrDefault(
                    igniteException.groupCode(),
                    SqlExceptionHandler::otherIgniteException
            ).apply(igniteException);

        } else if (unwrappedCause instanceof IgniteCheckedException) {
            IgniteCheckedException checkedError = (IgniteCheckedException) unwrappedCause;

            errorComponent = fromCheckedIgniteException(UNRECOGNIZED_ERROR_MESSAGE, checkedError);
        } else {
            LOG.warn("Unrecognized exception", e);

            errorComponent = ErrorUiComponent.builder()
                    .header("Unrecognized exception")
                    .details(String.valueOf(unwrappedCause))
                    .build();
        }

        err.write(errorComponent.render());
        return 1;
    }

    private static ErrorUiComponent sqlErrUiComponent(IgniteException e) {
        String header;

        if (e.code() == Sql.STMT_PARSE_ERR) {
            header = "SQL query parsing error";
        } else if (e.code() == Sql.STMT_VALIDATION_ERR) {
            header = "SQL query validation error";
        } else if (e.code() == Sql.CONSTRAINT_VIOLATION_ERR) {
            header = "Constraint violation";
        } else if (e.code() == Sql.EXECUTION_CANCELLED_ERR) {
            header = "Query was cancelled";
        } else if (e.code() == Sql.RUNTIME_ERR) {
            header = "SQL query execution error";
        } else if (e.code() == Sql.MAPPING_ERR) {
            header = "Unable to map query on current cluster topology";
        } else if (e.code() == Sql.TX_CONTROL_INSIDE_EXTERNAL_TX_ERR) {
            header = "Unexpected SQL statement";
        } else {
            header = "SQL error";
        }

        return fromIgniteException(header, e);
    }

    private static ErrorUiComponent fromIgniteException(String header, IgniteException e) {
        // Header
        // GROUP_NAME-CODE: ... 
        return ErrorUiComponent.builder()
                .header(header)
                .errorCode(String.valueOf(e.code()))
                .traceId(e.traceId())
                .details(e.codeAsString() + ": " + extractCauseMessage(e.getMessage()))
                .build();
    }

    private static ErrorUiComponent fromCheckedIgniteException(String header, IgniteCheckedException e) {
        // Header
        // GROUP_NAME-CODE: ...
        return ErrorUiComponent.builder()
                .header(header)
                .errorCode(String.valueOf(e.code()))
                .traceId(e.traceId())
                .details(e.codeAsString() + ": " + extractCauseMessage(e.getMessage()))
                .build();
    }

    private static ErrorUiComponent otherIgniteException(IgniteException e) {
        // GROUP_NAME error
        // GROUP_NAME-CODE: ... 
        return fromIgniteException(e.groupName() + " error", e);
    }

    @Override
    public Class<SQLException> applicableException() {
        return SQLException.class;
    }
}
