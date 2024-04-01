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

package org.apache.ignite.internal.benchmark;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark measures the time for sequential execution of different sets of statements and the time
 * for executing the same set of statements, but running them as a script.
 *
 * <p>Include the following cases:
 * <ol>
 *     <li>{@code INSERT} the same key into different tables.</li>
 *     <li>{@code SELECT COUNT(*)} from different tables.</li>
 *     <li>{@code SELECT} using single key from multiple tables.</li>
 *     <li>Multiple {@code SELECT} by key from one table.</li>
 * </ol>
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SqlMultiStatementBenchmark extends AbstractMultiNodeBenchmark {
    private static final int TABLE_SIZE = 10_000;

    private static final int PAGE_SIZE = 1024;

    @Param({"1", "2"})
    private int clusterSize;

    @Param({"1", "2", "4"})
    private static int statementsCount;

    /** Creates tables. */
    @Setup
    public void createTables() {
        for (int i = 0; i < statementsCount; i++) {
            createTable("T" + i);
        }
    }

    /** Benchmark for sequential {@code INSERT}. */
    @Benchmark
    public void insert(InsertState state, Blackhole bh) {
        state.executeQuery(bh);
    }

    /** Benchmark for script {@code INSERT}. */
    @Benchmark
    public void insertScript(InsertState state, Blackhole bh) {
        state.executeScript(bh);
    }

    /** Benchmark for sequential {@code COUNT}. */
    @Benchmark
    public void count(CountState state, Blackhole bh) {
        state.executeQuery(bh);
    }

    /** Benchmark for script {@code COUNT}. */
    @Benchmark
    public void countScript(CountState state, Blackhole bh) {
        state.executeScript(bh);
    }

    /** Benchmark for sequential single key {@code SELECT}. */
    @Benchmark
    public void selectMultipleTables(SelectSingleKeyMultipleTablesState state, Blackhole bh) {
        state.executeQuery(bh);
    }

    /** Benchmark for script single key {@code SELECT}. */
    @Benchmark
    public void selectMultipleTablesScript(SelectSingleKeyMultipleTablesState state, Blackhole bh) {
        state.executeScript(bh);
    }

    /** Benchmark for sequential single table {@code SELECT}. */
    @Benchmark
    public void selectMultipleKeys(SelectMultipleKeysSingleTableState state, Blackhole bh) {
        state.executeQuery(bh);
    }

    /** Benchmark for script single table {@code SELECT}. */
    @Benchmark
    public void selectMultipleKeysScript(SelectMultipleKeysSingleTableState state, Blackhole bh) {
        state.executeScript(bh);
    }

    /**
     * Benchmark state for {@link #insert(InsertState, Blackhole)} and
     * {@link #insertScript(InsertState, Blackhole)} benchmarks.
     */
    @State(Scope.Benchmark)
    public static class InsertState {
        private QueryRunner queryRunner;
        private Parameters parameters;

        /** Generates required statements.*/
        @Setup
        public void setUp() {
            parameters = new Parameters(statementsCount, n -> createInsertStatement("T" + n));
            queryRunner = new QueryRunner(
                    clusterNode.queryEngine(),
                    clusterNode.transactions(),
                    PAGE_SIZE
            );
        }

        private int id = 0;

        void executeQuery(Blackhole bh) {
            int id0 = id++;

            for (int i = 0; i < statementsCount; i++) {
                Iterator<?> res = queryRunner.execQuery(parameters.statements.get(i), id0);

                while (res.hasNext()) {
                    bh.consume(res.next());
                }
            }
        }

        void executeScript(Blackhole bh) {
            int id0 = id++;

            for (int i = 0; i < statementsCount; i++) {
                parameters.scriptArgs[i] = id0;
            }

            Iterator<InternalSqlRow> res = queryRunner.execScript(parameters.script, parameters.scriptArgs);

            while (res.hasNext()) {
                bh.consume(res.next());
            }
        }

        private static String createInsertStatement(String tableName) {
            String insertQueryTemplate = "insert into {}({}, {}) values(?, {})";

            String fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
            String valQ = IntStream.range(1, 11).mapToObj(i -> "'" + FIELD_VAL + "'").collect(joining(","));

            return format(insertQueryTemplate, tableName, "ycsb_key", fieldsQ, valQ);
        }
    }

    /**
     * Benchmark state for {@link #count(CountState, Blackhole)} and
     * {@link #countScript(CountState, Blackhole)} benchmarks.
     */
    @State(Scope.Benchmark)
    public static class CountState {
        private QueryRunner queryRunner;
        private Parameters parameters;

        /** Generates required statements.*/
        @Setup
        public void setUp() {
            fillTables(statementsCount);

            parameters = new Parameters(statementsCount, n -> format("select count(*) from T{}", n));
            queryRunner = new QueryRunner(
                    clusterNode.queryEngine(),
                    clusterNode.transactions(),
                    PAGE_SIZE
            );
        }

        void executeQuery(Blackhole bh) {
            for (int i = 0; i < statementsCount; i++) {
                Iterator<?> res = queryRunner.execQuery(parameters.statements.get(i));

                while (res.hasNext()) {
                    bh.consume(res.next());
                }
            }
        }

        void executeScript(Blackhole bh) {
            Iterator<?> res = queryRunner.execScript(parameters.script);

            while (res.hasNext()) {
                bh.consume(res.next());
            }
        }
    }

    /**
     * Benchmark state for {@link #selectMultipleTables(SelectSingleKeyMultipleTablesState, Blackhole)} and
     * {@link #selectMultipleTablesScript(SelectSingleKeyMultipleTablesState, Blackhole)} benchmarks.
     */
    @State(Scope.Benchmark)
    public static class SelectSingleKeyMultipleTablesState {
        private final Random random = new Random();
        private QueryRunner queryRunner;
        private Parameters parameters;

        /** Generates required statements.*/
        @Setup
        public void setUp() {
            fillTables(statementsCount);

            parameters = new Parameters(statementsCount, n -> format("select * from T{} where ycsb_key=?", n));
            queryRunner = new QueryRunner(
                    clusterNode.queryEngine(),
                    clusterNode.transactions(),
                    PAGE_SIZE
            );
        }

        void executeQuery(Blackhole bh) {
            int key = random.nextInt(TABLE_SIZE);

            for (int i = 0; i < statementsCount; i++) {
                Iterator<?> res = queryRunner.execQuery(parameters.statements.get(i), key);

                while (res.hasNext()) {
                    bh.consume(res.next());
                }
            }
        }

        void executeScript(Blackhole bh) {
            int key = random.nextInt(TABLE_SIZE);

            for (int i = 0; i < statementsCount; i++) {
                parameters.scriptArgs[i] = key;
            }

            Iterator<?> res = queryRunner.execScript(parameters.script, parameters.scriptArgs);

            while (res.hasNext()) {
                bh.consume(res.next());
            }
        }
    }

    /**
     * Benchmark state for {@link #selectMultipleKeys(SelectMultipleKeysSingleTableState, Blackhole)} and
     * {@link #selectMultipleKeysScript(SelectMultipleKeysSingleTableState, Blackhole)} benchmarks.
     */
    @State(Scope.Benchmark)
    public static class SelectMultipleKeysSingleTableState {
        private final Random random = new Random();
        private QueryRunner queryRunner;
        private Parameters parameters;

        /** Generates required statements.*/
        @Setup
        public void setUp() {
            fillTables(statementsCount);

            parameters = new Parameters(statementsCount, ignore -> "select * from T0 where ycsb_key=?");
            queryRunner = new QueryRunner(
                    clusterNode.queryEngine(),
                    clusterNode.transactions(),
                    PAGE_SIZE
            );
        }

        void executeQuery(Blackhole bh) {
            for (int i = 0; i < statementsCount; i++) {
                Iterator<?> res = queryRunner.execQuery(parameters.statements.get(i), random.nextInt(TABLE_SIZE));

                while (res.hasNext()) {
                    bh.consume(res.next());
                }
            }
        }

        void executeScript(Blackhole bh) {
            for (int i = 0; i < statementsCount; i++) {
                parameters.scriptArgs[i] = random.nextInt(TABLE_SIZE);
            }

            Iterator<?> res = queryRunner.execScript(parameters.script, parameters.scriptArgs);

            while (res.hasNext()) {
                bh.consume(res.next());
            }
        }
    }

    private static class Parameters {
        private final List<String> statements = new ArrayList<>();
        private final String script;
        private final Object[] scriptArgs;

        private Parameters(int count, Function<Integer, String> statementGenerator) {
            IgniteStringBuilder buf = new IgniteStringBuilder();

            for (int i = 0; i < count; i++) {
                String statement = statementGenerator.apply(i);
                statements.add(statement);
                buf.app(statement).app(';');
            }

            script = buf.toString();
            scriptArgs = new Object[count];
        }
    }

    /** Executes SQL query/script using internal API. */
    private static class QueryRunner {
        private final SqlProperties props = SqlPropertiesHelper.newBuilder()
                .set(QueryProperty.ALLOWED_QUERY_TYPES, SqlQueryType.SINGLE_STMT_TYPES)
                .build();

        private final SqlProperties scriptProps = SqlPropertiesHelper.newBuilder()
                .set(QueryProperty.ALLOWED_QUERY_TYPES, SqlQueryType.ALL)
                .build();

        private final QueryProcessor queryProcessor;
        private final IgniteTransactions transactions;
        private final int pageSize;

        QueryRunner(QueryProcessor queryProcessor, IgniteTransactions transactions, int pageSize) {
            this.queryProcessor = queryProcessor;
            this.transactions = transactions;
            this.pageSize = pageSize;
        }

        Iterator<InternalSqlRow> execQuery(String sql, Object ... args) {
            AsyncSqlCursor<InternalSqlRow> cursor =
                    queryProcessor.queryAsync(props, transactions, null, sql, args).join();

            return new InternalResultsIterator(cursor, pageSize);
        }

        Iterator<InternalSqlRow> execScript(String sql, Object ... args) {
            AsyncSqlCursor<InternalSqlRow> cursor =
                    queryProcessor.queryAsync(scriptProps, transactions, null, sql, args).join();

            return new InternalResultsIterator(cursor, pageSize);
        }

        private static class InternalResultsIterator implements Iterator<InternalSqlRow> {
            private final int fetchSize;
            private BatchedResult<InternalSqlRow> res;
            private Iterator<InternalSqlRow> items;
            private AsyncSqlCursor<InternalSqlRow> cursor;

            InternalResultsIterator(AsyncSqlCursor<InternalSqlRow> cursor, int fetchSize) {
                this.cursor = cursor;
                this.fetchSize = fetchSize;
                this.items = fetchNext();
            }

            @Override
            public boolean hasNext() {
                if (items.hasNext()) {
                    return true;
                }

                if (res.hasMore()) {
                    items = fetchNext();
                } else if (cursor.hasNextResult()) {
                    cursor.closeAsync();

                    cursor = cursor.nextResult().join();
                    items = fetchNext();
                }

                boolean hasNext = items.hasNext();

                if (!hasNext) {
                    cursor.closeAsync();
                }

                return hasNext;
            }

            @Override
            public InternalSqlRow next() {
                return items.next();
            }

            private Iterator<InternalSqlRow> fetchNext() {
                res = cursor.requestNextAsync(fetchSize).join();

                return res.items().iterator();
            }
        }
    }

    private static void fillTables(int tablesCount) {
        int id = 0;

        Map<Tuple, Tuple> data = new HashMap<>();

        for (int i = 0; i < TABLE_SIZE; i++) {
            Tuple t = Tuple.create();
            for (int j = 1; j <= 10; j++) {
                t.set("field" + j, FIELD_VAL);
            }

            data.put(Tuple.create().set("ycsb_key", id++), t);
        }

        for (int i = 0; i < tablesCount; i++) {
            clusterNode.tables().table("T" + i).keyValueView().putAll(null, data);
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + SqlMultiStatementBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }
}
