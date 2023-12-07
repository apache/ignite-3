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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.SyncResultSetAdapter;
import org.apache.ignite.internal.table.criteria.SqlSerializer;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ClosableCursor;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncClosableCursor;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for client views.
 */
abstract class AbstractClientView<R> implements CriteriaQuerySource<R> {
    /** Underlying table. */
    protected final ClientTable tbl;

    /**
     * Constructor.
     *
     * @param tbl Underlying table.
     */
    AbstractClientView(ClientTable tbl) {
        assert tbl != null;

        this.tbl = tbl;
    }

    /**
     * Get index mapping.
     *
     * @param columns Columns to map.
     * @param metadata Metadata for query results.
     * @return Index mapping.
     */
    protected static List<Integer> indexMapping(ClientColumn[] columns, int from, int to, @Nullable ResultSetMetadata metadata) {
        if (metadata == null) {
            throw new IllegalStateException("Metadata can't be null.");
        }

        return Arrays.stream(columns, from, to)
                .map(ClientColumn::name)
                .map((columnName) -> {
                    var rowIdx = metadata.indexOf(columnName);

                    if (rowIdx == -1) {
                        throw new IgniteException(Sql.RUNTIME_ERR, "Missing required column in query results: " + columnName);
                    }

                    return rowIdx;
                })
                .collect(toList());
    }

    /**
     * Criteria query over cache entries.
     *
     * @param tx Transaction to execute the query within or {@code null}.
     * @param statement SQL statement to execute.
     * @param arguments Arguments for the statement.
     * @throws SqlException If failed.
     */
    abstract CompletableFuture<AsyncResultSet<R>> executeQueryAsync(
            @Nullable Transaction tx,
            Statement statement,
            @Nullable Object... arguments
    );

    /** {@inheritDoc} */
    @Override
    public ClosableCursor<R> queryCriteria(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            CriteriaQueryOptions opts
    ) {
        var ser = new SqlSerializer.Builder()
                .tableName(tbl.name())
                .where(criteria)
                .build();

        var statement = tbl.sql().statementBuilder().query(ser.toString()).pageSize(opts.pageSize()).build();

        return new SyncResultSetAdapter<>(executeQueryAsync(tx, statement, ser.getArguments()).join());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncClosableCursor<R>> queryCriteriaAsync(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            CriteriaQueryOptions opts
    ) {
        var ser = new SqlSerializer.Builder()
                .tableName(tbl.name())
                .where(criteria)
                .build();

        var statement = tbl.sql().statementBuilder().query(ser.toString()).pageSize(opts.pageSize()).build();

        return executeQueryAsync(tx, statement, ser.getArguments())
                .thenApply(identity());
    }
}
