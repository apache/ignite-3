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

package org.apache.ignite.internal.client.table;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.client.ClientUtils.sync;
import static org.apache.ignite.internal.table.criteria.CriteriaExceptionMapperUtil.mapToPublicCriteriaException;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.util.IgniteNameUtils.parseSimpleName;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.sql.StatementBuilderImpl;
import org.apache.ignite.internal.table.criteria.CursorAdapter;
import org.apache.ignite.internal.table.criteria.QueryCriteriaAsyncCursor;
import org.apache.ignite.internal.table.criteria.SqlSerializer;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for client views.
 */
abstract class AbstractClientView<T> implements CriteriaQuerySource<T> {
    /** Underlying table. */
    protected final ClientTable tbl;
    protected final ClientSql sql;

    /**
     * Constructor.
     *
     * @param tbl Underlying table.
     * @param sql Sql facade.
     */
    AbstractClientView(ClientTable tbl, ClientSql sql) {
        assert tbl != null;
        assert sql != null;

        this.tbl = tbl;
        this.sql = sql;
    }

    /**
     * Map columns to it's names.
     *
     * @param columns Target columns.
     * @return Column names.
     */
    protected static String[] columnNames(ClientColumn[] columns) {
        String[] columnNames = new String[columns.length];

        for (int i = 0; i < columns.length; i++) {
            columnNames[i] = columns[i].name();
        }

        return columnNames;
    }

    /**
     * Construct SQL query and arguments for prepare statement.
     *
     * @param tableName Table name.
     * @param columns Table columns.
     * @param criteria The predicate to filter entries or {@code null} to return all entries from the underlying table.
     * @return SQL query and it's arguments.
     */
    protected static SqlSerializer createSqlSerializer(String tableName, ClientColumn[] columns, @Nullable Criteria criteria) {
        Set<String> columnNames = Arrays.stream(columns)
                .map(ClientColumn::name)
                .collect(toSet());

        return new SqlSerializer.Builder()
                .tableName(parseSimpleName(tableName))
                .columns(columnNames)
                .where(criteria)
                .build();
    }

    /**
     * Create conversion function for objects contained by result set to criteria query objects.
     *
     * @param meta Result set columns' metadata.
     * @param schema Schema.
     * @return Conversion function (if {@code null} conversions isn't required).
     */
    protected @Nullable Function<SqlRow, T> queryMapper(ResultSetMetadata meta, ClientSchema schema) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<T> query(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts) {
        return new CursorAdapter<>(sync(queryAsync(tx, criteria, null, opts)));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncCursor<T>> queryAsync(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts
    ) {
        CriteriaQueryOptions opts0 = opts == null ? CriteriaQueryOptions.DEFAULT : opts;

        return tbl.getLatestSchema()
                .thenCompose((schema) -> {
                    SqlSerializer ser = createSqlSerializer(tbl.name(), schema.columns(), criteria);

                    Statement statement = new StatementBuilderImpl().query(ser.toString()).pageSize(opts0.pageSize()).build();

                    return sql.executeAsync(tx, statement, ser.getArguments())
                            .<AsyncCursor<T>>thenApply(resultSet -> {
                                ResultSetMetadata meta = resultSet.metadata();

                                assert meta != null : "Metadata can't be null.";

                                return new QueryCriteriaAsyncCursor<>(resultSet, queryMapper(meta, schema), () -> {/* NO-OP */});
                            });
                })
                .exceptionally(th -> {
                    throw new CompletionException(mapToPublicCriteriaException(unwrapCause(th)));
                });
    }
}
