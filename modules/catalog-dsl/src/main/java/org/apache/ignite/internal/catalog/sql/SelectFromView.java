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

package org.apache.ignite.internal.catalog.sql;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;

class SelectFromView<T> extends AbstractCatalogQuery<List<T>> {
    private final String viewName;

    private final List<String> columns;

    private final List<Option> whereOptions = new ArrayList<>();

    private final Function<SqlRow, T> mapper;

    SelectFromView(IgniteSql sql, List<String> columns, String viewName, Option whereOption, Function<SqlRow, T> mapper) {
        this(sql, columns, viewName, List.of(whereOption), mapper);
    }

    SelectFromView(IgniteSql sql, List<String> columns, String viewName, List<Option> whereOptions, Function<SqlRow, T> mapper) {
        super(sql);
        this.viewName = viewName;
        this.columns = columns;
        this.whereOptions.addAll(whereOptions);
        this.mapper = mapper;
    }

    @Override
    public CompletableFuture<List<T>> executeAsync() {
        return sql.executeAsync((Transaction) null, toString()).thenCompose(resultSet -> {
            List<T> result = new ArrayList<>();
            return iterate(resultSet, mapper, result).thenApply(unused -> result);
        });
    }

    static <T> CompletableFuture<List<T>> collectResults(IgniteSql sql, String query, Function<SqlRow, T> mapper, Object... params) {
        return sql.executeAsync(null, query, params).thenCompose(resultSet -> {
            List<T> result = new ArrayList<>();
            return iterate(resultSet, mapper, result).thenApply(unused -> result);
        });
    }

    private static <T> CompletableFuture<Void> iterate(AsyncResultSet<SqlRow> resultSet, Function<SqlRow, T> mapper, List<T> result) {
        for (SqlRow row : resultSet.currentPage()) {
            result.add(mapper.apply(row));
        }
        if (resultSet.hasMorePages()) {
            return resultSet.fetchNextPage().thenCompose(nextPage -> iterate(nextPage, mapper, result));
        } else {
            return nullCompletedFuture();
        }
    }

    @Override
    // Noop
    protected List<T> result() {
        return Collections.emptyList();
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.sql("SELECT " + String.join(", ", columns) + " FROM SYSTEM." + viewName + " ");

        if (!whereOptions.isEmpty()) {
            ctx.sql("WHERE ");
            for (int i = 0; i < whereOptions.size(); i++) {
                Option option = whereOptions.get(i);
                if (i > 0) {
                    ctx.sql(" AND ");
                }
                option.accept(ctx);
            }
        }
    }
}
