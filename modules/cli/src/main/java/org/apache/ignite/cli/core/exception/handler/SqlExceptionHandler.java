package org.apache.ignite.cli.core.exception.handler;

import java.sql.SQLException;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception handler for {@link SQLException}.
 */
public class SqlExceptionHandler implements ExceptionHandler<SQLException> {
    private static final Logger log = LoggerFactory.getLogger(SqlExceptionHandler.class);

    public static final String PARSING_ERROR_MESSAGE = "SQL query parsing error.";
    public static final String INVALID_PARAMETER_MESSAGE = "Invalid parameter value.";
    public static final String CLIENT_CONNECTION_FAILED_MESSAGE = "Connection failed.";
    public static final String CONNECTION_BROKE_MESSAGE = "Connection error.";

    @Override
    public void handle(ExceptionWriter err, SQLException e) {
        switch (e.getSQLState()) {
            case SqlStateCode.CONNECTION_FAILURE:
            case SqlStateCode.CONNECTION_CLOSED:
            case SqlStateCode.CONNECTION_REJECTED:
                err.write(CONNECTION_BROKE_MESSAGE);
                break;
            case SqlStateCode.PARSING_EXCEPTION:
                err.write(PARSING_ERROR_MESSAGE);
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
    }

    @Override
    public Class<SQLException> applicableException() {
        return SQLException.class;
    }
}
