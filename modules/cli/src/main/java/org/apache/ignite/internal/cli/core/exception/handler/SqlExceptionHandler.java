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

import static org.apache.ignite.lang.ErrorGroup.extractCauseMessage;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.style.component.ErrorUiComponent;
import org.apache.ignite.internal.cli.core.style.component.ErrorUiComponent.ErrorComponentBuilder;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;

/**
 * Exception handler for {@link SQLException}.
 */
public class SqlExceptionHandler implements ExceptionHandler<SQLException> {
    private static final IgniteLogger LOG = Loggers.forClass(SqlExceptionHandler.class);

    public static final String PARSING_ERROR_MESSAGE = "SQL query parsing error";

    public static final String INVALID_PARAMETER_MESSAGE = "Invalid parameter value";

    public static final String CLIENT_CONNECTION_FAILED_MESSAGE = "Connection failed";

    public static final String CONNECTION_BROKE_MESSAGE = "Connection error";

    public static final String UNRECOGNIZED_ERROR_MESSAGE = "Unrecognized error while processing SQL query ";

    private final Map<Integer, Function<IgniteException, ErrorComponentBuilder>> sqlExceptionMappers = new HashMap<>();

    /** Default constructor. */
    public SqlExceptionHandler() {
        sqlExceptionMappers.put(Client.CONNECTION_ERR, SqlExceptionHandler::connectionErrUiComponent);
        sqlExceptionMappers.put(Sql.STMT_PARSE_ERR, SqlExceptionHandler::sqlParseErrUiComponent);
    }

    private static ErrorComponentBuilder sqlParseErrUiComponent(IgniteException e) {
        return fromExWithHeader(PARSING_ERROR_MESSAGE, e.code(), e.traceId(), e.getMessage());
    }

    private static ErrorComponentBuilder unrecognizedErrComponent(IgniteException e) {
        return fromExWithHeader(UNRECOGNIZED_ERROR_MESSAGE, e.code(), e.traceId(), e.getMessage());
    }

    private static ErrorComponentBuilder connectionErrUiComponent(IgniteException e) {
        if (e.getCause() instanceof IgniteClientConnectionException) {
            IgniteClientConnectionException cause = (IgniteClientConnectionException) e.getCause();
            return fromExWithHeader(CLIENT_CONNECTION_FAILED_MESSAGE, cause.code(), cause.traceId(), cause.getMessage());
        }

        return fromExWithHeader(CLIENT_CONNECTION_FAILED_MESSAGE, e.code(), e.traceId(), e.getMessage());
    }

    private static ErrorComponentBuilder fromExWithHeader(String header, int errorCode, UUID traceId, String message) {
        return ErrorUiComponent.builder()
                .header(header)
                .errorCode(String.valueOf(errorCode))
                .traceId(traceId)
                .details(extractCauseMessage(message));
    }

    @Override
    public int handle(ExceptionWriter err, SQLException e) {
        Throwable unwrappedCause = ExceptionUtils.unwrapCause(e.getCause());
        if (unwrappedCause instanceof IgniteException) {
            return handleIgniteException(err, (IgniteException) unwrappedCause);
        }

        if (unwrappedCause instanceof IgniteCheckedException) {
            return handleIgniteCheckedException(err, (IgniteCheckedException) unwrappedCause);
        }

        var errorComponentBuilder = ErrorUiComponent.builder();

        switch (e.getSQLState()) {
            case SqlStateCode.CONNECTION_FAILURE:
            case SqlStateCode.CONNECTION_CLOSED:
            case SqlStateCode.CONNECTION_REJECTED:
                errorComponentBuilder.header(CONNECTION_BROKE_MESSAGE).verbose(extractCauseMessage(e.getMessage()));
                break;
            case SqlStateCode.PARSING_EXCEPTION:
                errorComponentBuilder.header(PARSING_ERROR_MESSAGE).details(extractCauseMessage(e.getMessage()));
                break;
            case SqlStateCode.INVALID_PARAMETER_VALUE:
                errorComponentBuilder.header(INVALID_PARAMETER_MESSAGE).verbose(extractCauseMessage(e.getMessage()));
                break;
            case SqlStateCode.CLIENT_CONNECTION_FAILED:
                errorComponentBuilder.header(CLIENT_CONNECTION_FAILED_MESSAGE).verbose(extractCauseMessage(e.getMessage()));
                break;
            default:
                LOG.error("Unrecognized error", e);
                errorComponentBuilder.header("SQL query execution error").details(e.getMessage());
        }

        err.write(errorComponentBuilder.build().render());
        return 1;
    }

    /** Handles IgniteException that has more information like error code and trace id. */
    private int handleIgniteException(ExceptionWriter err, IgniteException e) {
        var errorComponentBuilder = sqlExceptionMappers.getOrDefault(e.code(), SqlExceptionHandler::unrecognizedErrComponent);

        String renderedError = errorComponentBuilder.apply(e).build().render();
        err.write(renderedError);

        return 1;
    }

    private static int handleIgniteCheckedException(ExceptionWriter err, IgniteCheckedException e) {
        String renderedError = fromExWithHeader(UNRECOGNIZED_ERROR_MESSAGE, e.code(), e.traceId(), e.getMessage())
                .build().render();
        err.write(renderedError);

        return 1;
    }

    @Override
    public Class<SQLException> applicableException() {
        return SQLException.class;
    }
}
