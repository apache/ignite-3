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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.api.AsyncResultSetImpl;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
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
 * Benchmark compares the performance of executing different sets of statements sequentially
 * with executing the same set of statements as a script.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SqlScriptBenchmark extends AbstractMultiNodeBenchmark {
    private static final int TABLE_SIZE = 10_000;
    // TODO
    private static final int PAGE_SIZE = 1024;

    private Session session;

    private QueryProcessor queryProc;

    //@Param({"1", "2", "3"})
    @Param({"1"})
    private int clusterSize;

    @Param({"1"})
    private int statementsCount;

    private Map<StatementType, List<String>> statements = new LinkedHashMap<>();

//    class Statements {
//
//    }
//    private List<String> statements = new ArrayList<>();

    @Setup
    public void setUp() {
        KeyValueView<Tuple, Tuple> keyValueView = clusterNode.tables().table(TABLE_NAME).keyValueView();

        Tuple payload = Tuple.create();
        for (int j = 1; j <= 10; j++) {
            payload.set("field" + j, FIELD_VAL);
        }

        int batchSize = 1_000;
        Map<Tuple, Tuple> batch = new HashMap<>();
        for (int i = 0; i < TABLE_SIZE; i++) {
            batch.put(Tuple.create().set("ycsb_key", i), payload);

            if (batch.size() == batchSize) {
                keyValueView.putAll(null, batch);

                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            keyValueView.putAll(null, batch);

            batch.clear();
        }

        session = clusterNode.sql().createSession();

        queryProc = clusterNode.queryEngine();

        for (StatementType type : StatementType.values()) {
            for (int i = 0; i < statementsCount; i++) {
                statements.computeIfAbsent(type, list -> new ArrayList<>()).add(generateQuery(type, i));
            }
        }

        execStatements.addAll(statements.get(StatementType.CREATE));
        execStatements.addAll(statements.get(StatementType.INSERT));
        execStatements.addAll(statements.get(StatementType.SELECT_ALL));
        execStatements.addAll(statements.get(StatementType.DROP));

        IgniteStringBuilder buf = new IgniteStringBuilder();

        execStatements.forEach(str -> {
            buf.app(str).app(';');
        });

        execStatementScript = buf.toString();

        //
        String sql = format("select * from {}", TABLE_NAME);
        for (int i = 0; i < statementsCount; i++) {
            execSelectStatements.add(sql);
        }

        buf.setLength(0);

        execSelectStatements.forEach(str -> {
            buf.app(str).app(';');
        });

        execSelectStatementScript = buf.toString();
    }

    private List<String> execStatements = new ArrayList<>();
    private String execStatementScript;

    private List<String> execSelectStatements = new ArrayList<>();
    private String execSelectStatementScript;

    enum StatementType {
        CREATE,
        SELECT_ALL,
        INSERT,
        DROP
    }

    private String generateQuery(StatementType statementType, int idx) {
        switch (statementType) {
            case CREATE:
                return format("CREATE TABLE T{} (ID INT PRIMARY KEY, VAL VARCHAR)", idx);
            case DROP:
                return format("DROP TABLE T{}", idx);

            case SELECT_ALL:
                return format("SELECT * FROM T{}", idx);

            case INSERT:
                return format("INSERT INTO T{} VALUES (0, 0)", idx);

            default:
                throw new IllegalArgumentException("Unknown type " + statementType);
        }
    }

//    /** Benchmark that measures performance of `SELECT count(*)` query over entire table. */
//    @Benchmark
//    public void complexSingle(Blackhole bh) {
//        for (String sql : execStatements) {
//            try (var rs = session.execute(null, sql)) {
//                while (rs.hasNext()) {
//                    bh.consume(rs.next());
//                }
//            }
//        }
//    }
//
//    @Benchmark
//    public void complexScript(Blackhole bh) {
//        executeScript(execStatementScript);
//    }

    @Benchmark
    public void selectSingle(Blackhole bh) {
        for (String sql : execSelectStatements) {
            try (var rs = session.execute(null, sql)) {
                while (rs.hasNext()) {
                    bh.consume(rs.next());
                }
            }
        }
    }

    @Benchmark
    public void selectScript(Blackhole bh) {
        executeScript(execSelectStatementScript, bh);
    }

    void executeScript(String sql, Blackhole bh) {
        AsyncSqlCursor<InternalSqlRow> cursor = queryProc.queryScriptAsync(
                SqlPropertiesHelper.emptyProperties(), clusterNode.transactions(), null, sql).join();

        for (; ; ) {
            BatchedResult<InternalSqlRow> res;
            do {
                res = cursor.requestNextAsync(PAGE_SIZE).join();

                for (InternalSqlRow row : res.items()) {
                    bh.consume(row);
                }
            } while (res.hasMore());

            cursor.closeAsync();

            if (!cursor.hasNextResult())
                break;

            cursor = cursor.nextResult().join();
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + SqlScriptBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }
}
