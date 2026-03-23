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

package org.apache.ignite.client.handler;

import static org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode.UNSUPPORTED_OPERATION;
import static org.apache.ignite.internal.sql.engine.SqlQueryType.DDL;
import static org.apache.ignite.internal.sql.engine.SqlQueryType.KILL;
import static org.apache.ignite.internal.sql.engine.SqlQueryType.TX_CONTROL;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.TraceableException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Contains common methods used to process jdbc requests.
 */
abstract class JdbcHandlerBase {
    /** {@link SqlQueryType}s allowed in JDBC select statements. **/
    public static final Set<SqlQueryType> SELECT_STATEMENT_QUERIES = EnumSet.of(
            SqlQueryType.QUERY,
            SqlQueryType.EXPLAIN
    );

    /** {@link SqlQueryType}s types that return 0 in executeUpdate and execute / getUpdateCount. **/
    public static final Set<SqlQueryType> ZERO_UPDATE_COUNT_QUERIES = EnumSet.of(DDL, KILL, TX_CONTROL);

    /** Logger. */
    private final IgniteLogger log;

    /** Client registry resources. */
    protected final ClientResourceRegistry resources;

    JdbcHandlerBase(ClientResourceRegistry resources) {
        this.resources = resources;
        this.log = Loggers.forClass(this.getClass());
    }

    /**
     * Creates jdbc result for the cursor.
     *
     * @param cur Sql cursor for query.
     * @param pageSize Page size.
     * @return JdbcQuerySingleResult filled with first batch of data.
     */
    CompletionStage<JdbcQuerySingleResult> createJdbcResult(AsyncSqlCursor<InternalSqlRow> cur, int pageSize) {
        return cur.requestNextAsync(pageSize).thenApply(batch -> {
            Long cursorId = null;
            if (cur.hasNextResult()) {
                // in case of multi statement we need to save cursor in resources, so later we can derive it and
                // move to the next result
                try {
                    cursorId = resources.put(new ClientResource(cur, cur::closeAsync));
                } catch (IgniteInternalCheckedException e) {
                    cur.closeAsync();

                    return new JdbcQuerySingleResult(Response.STATUS_FAILED,
                            "Unable to store query cursor.");
                }
            }

            SqlQueryType queryType = cur.queryType();
            if (queryType.hasRowSet()) {
                if (cursorId == null && batch.hasMore()) {
                    // more fetches are expected, so let's keep the cursor in resources
                    try {
                        cursorId = resources.put(new ClientResource(cur, cur::closeAsync));
                    } catch (IgniteInternalCheckedException e) {
                        cur.closeAsync();

                        return new JdbcQuerySingleResult(Response.STATUS_FAILED,
                                "Unable to store query cursor.");
                    }
                }

                List<ColumnMetadata> columns = cur.metadata().columns();

                return buildSingleRequest(batch, columns, cursorId, cur.hasNextResult());
            } else if (queryType.returnsAffectedRows()) {
                boolean hasMoreData = batch.hasMore();

                if (!validateDmlResult(cur.metadata(), hasMoreData)) {
                    return new JdbcQuerySingleResult(Response.STATUS_FAILED, "Unexpected result for DML query");
                }

                long updCount = (long) batch.items().get(0).get(0);

                return new JdbcQuerySingleResult(cursorId, updCount, cur.hasNextResult());
            } else if (ZERO_UPDATE_COUNT_QUERIES.contains(queryType)) {
                return new JdbcQuerySingleResult(cursorId, 0, cur.hasNextResult());
            } else {
                return new JdbcQuerySingleResult(UNSUPPORTED_OPERATION,
                        "Query type is not supported yet [queryType=" + queryType + ']');
            }
        });
    }

    private static JdbcQuerySingleResult buildSingleRequest(
            BatchedResult<InternalSqlRow> batch,
            List<ColumnMetadata> columns,
            @Nullable Long cursorId,
            boolean hasNextResult
    ) {
        List<BinaryTupleReader> rows = new ArrayList<>(batch.items().size());
        for (InternalSqlRow item : batch.items()) {
            rows.add(item.asBinaryTuple());
        }

        List<JdbcColumnMeta> meta = new ArrayList<>(columns.size());

        for (ColumnMetadata column : columns) {
            meta.add(createColumnMetadata(column));
        }

        return new JdbcQuerySingleResult(cursorId, rows, meta, batch.hasMore(), hasNextResult);
    }

    JdbcQuerySingleResult createErrorResult(String logMessage, Throwable origin, @Nullable String errMessagePrefix) {
        Throwable ex = ExceptionUtils.unwrapCause(origin);

        log.debug(logMessage, ex);

        String errorMessage;

        if (ex instanceof TraceableException && ((TraceableException) ex).code() == Sql.TX_CONTROL_INSIDE_EXTERNAL_TX_ERR) {
            errorMessage = "Transaction control statements are not supported when autocommit mode is disabled";
        } else {
            errorMessage = getErrorMessage(ex);
        }

        return new JdbcQuerySingleResult(Response.STATUS_FAILED, (errMessagePrefix == null ? "" : errMessagePrefix) + errorMessage);
    }

    /**
     * Validate dml result. Check if it stores only one value of Long type.
     *
     * @param meta Fetched data from cursor.
     * @param next  HasNext flag.
     * @return Boolean value indicates if data is valid or not.
     */
    private static boolean validateDmlResult(ResultSetMetadata meta, boolean next) {
        if (next) {
            return false;
        }

        if (meta.columns().size() != 1) {
            return false;
        }

        return meta.columns().get(0).type() == ColumnType.INT64;
    }

    /**
     * Get a message of given exception for further sending to the client.
     *
     * @param t Throwable.
     * @return String filled with exception message.
     */
    @Nullable static String getErrorMessage(Throwable t) {
        Throwable cause = ExceptionUtils.unwrapCause(t);
        return cause.getMessage();
    }

    /**
     * Create Jdbc representation of column metadata from given origin and RelDataTypeField field.
     *
     * @param fldMeta field metadata contains info about column.
     * @return JdbcColumnMeta object.
     */
    private static JdbcColumnMeta createColumnMetadata(ColumnMetadata fldMeta) {
        ColumnMetadata.ColumnOrigin origin = fldMeta.origin();

        String schemaName = null;
        String tblName = null;
        String colName = null;

        if (origin != null) {
            schemaName = origin.schemaName();
            tblName = origin.tableName();
            colName = origin.columnName();
        }

        return new JdbcColumnMeta(
                fldMeta.name(),
                schemaName,
                tblName,
                colName,
                fldMeta.type(),
                fldMeta.precision(),
                fldMeta.scale(),
                fldMeta.nullable()
        );
    }
}
