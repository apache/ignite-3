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

package org.apache.ignite.client.fakes;

import static org.apache.ignite.internal.sql.engine.QueryProperty.DEFAULT_SCHEMA;
import static org.apache.ignite.internal.sql.engine.QueryProperty.QUERY_TIMEOUT;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.util.ListToInternalSqlRowAdapter;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Fake {@link AsyncSqlCursor}.
 */
public class FakeCursor implements AsyncSqlCursor<InternalSqlRow> {
    private final String qry;
    private final List<ColumnMetadata> columns = new ArrayList<>();
    private final List<InternalSqlRow> rows = new ArrayList<>();

    FakeCursor(String qry, SqlProperties properties, Object[] params, FakeIgniteQueryProcessor proc) {
        this.qry = qry;

        if ("SELECT PROPS".equals(qry)) {
            columns.add(new FakeColumnMetadata("name", ColumnType.STRING));
            columns.add(new FakeColumnMetadata("val", ColumnType.STRING));

            rows.add(getRow("schema", properties.get(DEFAULT_SCHEMA)));
            rows.add(getRow("timeout", String.valueOf(properties.get(QUERY_TIMEOUT))));
        } else if ("SELECT META".equals(qry)) {
            columns.add(new FakeColumnMetadata("BOOL", ColumnType.BOOLEAN));
            columns.add(new FakeColumnMetadata("INT8", ColumnType.INT8));
            columns.add(new FakeColumnMetadata("INT16", ColumnType.INT16));
            columns.add(new FakeColumnMetadata("INT32", ColumnType.INT32));
            columns.add(new FakeColumnMetadata("INT64", ColumnType.INT64));
            columns.add(new FakeColumnMetadata("FLOAT", ColumnType.FLOAT));
            columns.add(new FakeColumnMetadata("DOUBLE", ColumnType.DOUBLE));
            columns.add(new FakeColumnMetadata("DECIMAL", ColumnType.DECIMAL, 1, 2,
                    true, new ColumnOrigin("SCHEMA1", "TBL2", "BIG_DECIMAL")));
            columns.add(new FakeColumnMetadata("DATE", ColumnType.DATE));
            columns.add(new FakeColumnMetadata("TIME", ColumnType.TIME));
            columns.add(new FakeColumnMetadata("DATETIME", ColumnType.DATETIME));
            columns.add(new FakeColumnMetadata("TIMESTAMP", ColumnType.TIMESTAMP));
            columns.add(new FakeColumnMetadata("UUID", ColumnType.UUID));
            columns.add(new FakeColumnMetadata("BITMASK", ColumnType.BITMASK));
            columns.add(new FakeColumnMetadata("BYTE_ARRAY", ColumnType.BYTE_ARRAY));
            columns.add(new FakeColumnMetadata("PERIOD", ColumnType.PERIOD));
            columns.add(new FakeColumnMetadata("DURATION", ColumnType.DURATION));
            columns.add(new FakeColumnMetadata("NUMBER", ColumnType.NUMBER));

            var row = getRow(
                    true,
                    Byte.MIN_VALUE,
                    Short.MIN_VALUE,
                    Integer.MIN_VALUE,
                    Long.MIN_VALUE,
                    1.3f,
                    1.4d,
                    BigDecimal.valueOf(145),
                    LocalDate.of(2001, 2, 3),
                    LocalTime.of(4, 5),
                    LocalDateTime.of(2001, 3, 4, 5, 6),
                    Instant.ofEpochSecond(987),
                    new UUID(0, 0),
                    BitSet.valueOf(new byte[0]),
                    new byte[1],
                    Period.of(10, 9, 8),
                    Duration.ofDays(11),
                    BigInteger.valueOf(42));

            rows.add(row);
        } else if ("SELECT LAST SCRIPT".equals(qry)) {
            rows.add(getRow(proc.lastScript));
            columns.add(new FakeColumnMetadata("script", ColumnType.STRING));
        } else {
            rows.add(getRow(1));
            columns.add(new FakeColumnMetadata("col1", ColumnType.INT32));
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<BatchedResult<InternalSqlRow>> requestNextAsync(int rows) {
        var batch = new ArrayList<>(this.rows);

        if ("SELECT PROPS".equals(qry)) {
            batch.add(getRow("pageSize", String.valueOf(rows)));
        }

        return CompletableFuture.completedFuture(new BatchedResult<>(batch, true));
    }

    @Override
    public SqlQueryType queryType() {
        return SqlQueryType.QUERY;
    }

    @Override
    public ResultSetMetadata metadata() {
        return new ResultSetMetadata() {
            @Override
            public List<ColumnMetadata> columns() {
                return columns;
            }

            @Override
            public int indexOf(String columnName) {
                return 0;
            }
        };
    }

    @Override
    public boolean hasNextResult() {
        return false;
    }

    @Override
    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> onClose() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> onFirstPageReady() {
        throw new UnsupportedOperationException();
    }

    private static InternalSqlRow getRow(Object... vals) {
        var list = new ArrayList<>(vals.length);
        Collections.addAll(list, vals);

        return new ListToInternalSqlRowAdapter(list);
    }

    private static class ColumnOrigin implements ColumnMetadata.ColumnOrigin {
        private final String schemaName;
        private final String tableName;
        private final String columnName;

        private ColumnOrigin(String schemaName, String tableName, String columnName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.columnName = columnName;
        }

        @Override
        public String schemaName() {
            return schemaName;
        }

        @Override
        public String tableName() {
            return tableName;
        }

        @Override
        public String columnName() {
            return columnName;
        }
    }
}
