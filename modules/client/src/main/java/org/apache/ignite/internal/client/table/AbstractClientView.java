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

import static org.apache.ignite.internal.table.criteria.CriteriaExceptionMapperUtil.mapToPublicCriteriaException;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.Arrays;
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
import org.apache.ignite.table.QualifiedName;
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
     * Create conversion function for objects contained by result set to criteria query objects.
     *
     * @param meta Result set columns' metadata.
     * @param schema Schema.
     * @return Conversion function (if {@code null} conversions isn't required).
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected @Nullable Function<SqlRow, T> queryMapper(ResultSetMetadata meta, ClientSchema schema) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<T> query(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts) {
        return new CursorAdapter<>(sync(queryAsync(tx, criteria, indexName, opts)));
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
                    SqlSerializer ser = new SqlSerializer.Builder()
                            .columns(Arrays.asList(columnNames(schema.columns())))
                            .tableName(tbl.qualifiedName())
                            .indexName(indexName != null ? QualifiedName.parse(indexName).objectName() : null)
                            .where(criteria)
                            .build();

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
