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

package org.apache.ignite.internal.sqllogic;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.base.Strings;
import com.google.common.collect.Streams;
import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.hamcrest.CoreMatchers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;

/**
 * Runs one SQL test script.
 */
public class SqlScriptRunner {

    /**
     * Provides dependencies required for {@link SqlScriptRunner}.
     */
    public interface RunnerRuntime {

        /** Returns a query executor. **/
        IgniteSql sql();

        /** Returns a logger. **/
        IgniteLogger log();

        /**
         * Returns a name of the current database engine.
         * This property is used by {@code skipif} and {@code onlyif} commands.
         * **/
        String engineName();
    }

    /** UTF-8 character name. */
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    /** Hashing label pattern. */
    private static final Pattern HASHING_PTRN = Pattern.compile("([0-9]+) values hashing to ([0-9a-fA-F]+)");

    /** Supported commands. **/
    private static final Map<String, ParseCommand> COMMANDS = Map.of(
            "statement", Statement::new,
            "query", Query::new,
            "loop", Loop::new,
            "endloop", EndLoop::new,
            "skipif", SkipIf::new,
            "onlyif", OnlyIf::new
    );

    /** NULL label. */
    private static final String NULL = "NULL";

    /** Test script path. */
    private final Path test;

    /** Runtime dependencies of a script runner. **/
    private final RunnerRuntime runnerRuntime;

    /**
     * Line separator to bytes representation.
     * NB: Don't use {@code System.lineSeparator()} here.
     * The separator is used to calculate hash by SQL results and mustn't be changed on the different platforms.
     */
    private static final byte[] LINE_SEPARATOR_BYTES = "\n".getBytes(UTF_8);

    /**
     * Script runner constructor.
     */
    public SqlScriptRunner(Path test, RunnerRuntime runnerRuntime) {
        this.test = test;
        this.runnerRuntime = runnerRuntime;
    }

    /**
     * Executes SQL file script.
     */
    public void run() throws Exception {
        var ctx = new ScriptContext(runnerRuntime);

        try (var script = new Script(test, COMMANDS, ctx)) {
            for (Command cmd : script) {
                try {
                    cmd.execute(ctx);
                } catch (Exception e) {
                    throw script.reportError("Command execution error", cmd.getClass().getSimpleName(), e);
                } finally {
                    ctx.loopVars.clear();
                }
            }
        }
    }

    /**
     * Script execution context.
     */
    private static final class ScriptContext {

        /** Query engine. */
        private final IgniteSql ignSql;

        /** Loop variables. */
        private final Map<String, Integer> loopVars = new HashMap<>();

        /** Logger. */
        private final IgniteLogger log;

        /** Database engine name. **/
        private final String engineName;

        /** String presentation of null's. */
        private String nullLbl = NULL;

        /** Comparator for "rowsort" sort mode. */
        private final Comparator<List<?>> rowComparator;

        /** Equivalent results store. */
        private final Map<String, Collection<String>> eqResStorage = new HashMap<>();

        /** Hash algo. */
        private final MessageDigest messageDigest;

        ScriptContext(RunnerRuntime runnerRuntime) {
            this.log = runnerRuntime.log();
            this.engineName = runnerRuntime.engineName();
            this.ignSql = runnerRuntime.sql();
            this.rowComparator = (r1, r2) -> {
                int rows = r1.size();

                for (int i = 0; i < rows; ++i) {
                    String s1 = SqlScriptRunner.toString(this, r1.get(i));
                    String s2 = SqlScriptRunner.toString(this, r2.get(i));

                    int comp = s1.compareTo(s2);

                    if (comp != 0) {
                        return comp;
                    }
                }

                return 0;
            };

            try {
                messageDigest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static List<List<?>> sql(ScriptContext ctx, String sql) {
        if (!ctx.loopVars.isEmpty()) {
            for (Map.Entry<String, Integer> loopVar : ctx.loopVars.entrySet()) {
                sql = sql.replaceAll("\\$\\{" + loopVar.getKey() + "\\}", loopVar.getValue().toString());
            }
        }

        ctx.log.info("Execute: " + sql);

        try (Session s = ctx.ignSql.createSession()) {
            try (ResultSet<SqlRow> rs = s.execute(null, sql)) {
                if (rs.hasRowSet()) {
                    return Streams.stream(rs).map(
                                    r -> {
                                        List<?> row = new ArrayList<>();

                                        for (int i = 0; i < rs.metadata().columns().size(); ++i) {
                                            row.add(r.value(i));
                                        }

                                        return row;
                                    })
                            .collect(Collectors.toList());
                } else if (rs.affectedRows() != -1) {
                    return Collections.singletonList(Collections.singletonList(rs.affectedRows()));
                } else {
                    return Collections.singletonList(Collections.singletonList(rs.wasApplied()));
                }
            }
        }
    }

    private static String toString(ScriptContext ctx, Object res) {
        if (res == null) {
            return ctx.nullLbl;
        } else if (res instanceof byte[]) {
            return ByteString.toString((byte[]) res, 16);
        } else if (res instanceof Map) {
            return mapToString(ctx, (Map<?, ?>) res);
        } else {
            return String.valueOf(res);
        }
    }

    private static String mapToString(ScriptContext ctx, Map<?, ?> map) {
        if (map == null) {
            return ctx.nullLbl;
        }

        List<String> entries = (new TreeMap<>(map)).entrySet().stream()
                .map(e -> toString(ctx, e.getKey()) + ":" + toString(ctx, e.getValue()))
                .collect(Collectors.toList());

        return "{" + String.join(", ", entries) + "}";
    }

    private static final class Script implements Iterable<Command>, AutoCloseable {

        private final String fileName;

        private final BufferedReader buffReader;

        private int lineNum;

        private final Map<String, ParseCommand> commands;

        private final ScriptContext ctx;

        Script(Path test, Map<String, ParseCommand> commands, ScriptContext ctx) throws IOException {
            this.fileName = test.getFileName().toString();
            this.commands = commands;
            this.buffReader = Files.newBufferedReader(test);
            this.ctx = ctx;
        }

        String nextLine() throws IOException {
            return nextLineWithoutTrim();
        }

        String nextLineWithoutTrim() throws IOException {
            String s = buffReader.readLine();

            lineNum++;

            return s;
        }

        boolean ready() throws IOException {
            return buffReader.ready();
        }

        ScriptPosition scriptPosition() {
            return new ScriptPosition(fileName, lineNum);
        }

        @Override
        public void close() throws Exception {
            buffReader.close();
        }

        @Nullable
        private Command nextCommand() {
            try {
                while (ready()) {
                    String line = nextLine();

                    if (Strings.isNullOrEmpty(line) || line.startsWith("#")) {
                        continue;
                    }

                    String[] tokens = line.split("\\s+");
                    if (tokens.length == 0) {
                        throw reportError("Invalid line", line);
                    }

                    String token = tokens[0];
                    return parseCommand(token, tokens, line);
                }

                return null;
            } catch (IOException e) {
                throw reportError("Can not read next command",  null, e);
            }
        }

        private Command parseCommand(String token, String[] tokens, String line) throws IOException {
            ParseCommand parser = commands.get(token);
            if (parser == null) {
                throw reportError("Unexpected command " + token,  line);
            }
            try {
                return parser.parse(this, ctx, tokens);
            } catch (Exception e) {
                throw reportError("Failed to parse a command", line, e);
            }
        }

        @NotNull
        @Override
        public Iterator<Command> iterator() {
            Command cmd0 = nextCommand();
            return new Iterator<>() {
                @Nullable
                private Command cmd = cmd0;

                @Override
                public boolean hasNext() {
                    return cmd != null;
                }

                @Override
                public Command next() {
                    if (cmd == null) {
                        throw new NoSuchElementException();
                    }

                    Command ret = cmd;

                    cmd = nextCommand();

                    return ret;
                }
            };
        }

        private ScriptException reportInvalidCommand(String error, String[] command) {
            var pos = scriptPosition();

            return new ScriptException(error, pos, Arrays.toString(command));
        }

        private ScriptException reportInvalidCommand(String error, String[] command, @Nullable Throwable cause) {
            var pos = scriptPosition();

            return new ScriptException(error, pos, Arrays.toString(command), cause);
        }

        private ScriptException reportError(String error, @Nullable String message) {
            var pos = scriptPosition();

            return new ScriptException(error, pos, message);
        }

        private ScriptException reportError(String error, @Nullable String message, @Nullable Throwable cause) {
            var pos = scriptPosition();

            return new ScriptException(error, pos, message, cause);
        }
    }

    private static final class ScriptPosition {

        final String fileName;

        final int lineNum;

        ScriptPosition(String fileName, int lineNum) {
            this.fileName = fileName;
            this.lineNum = lineNum;
        }

        @Override
        public String toString() {
            return '(' + fileName + ':' + lineNum + ')';
        }
    }

    /**
     * An exception with position in a script.
     */
    private static final class ScriptException extends RuntimeException {

        private static final long serialVersionUID = -5972259903486232854L;

        /**
         * Creates an exception with a message in the one of the following forms.
         * <ul>
         *     <li>When a message parameter is specified and is not empty: {@code error at position: message}</li>
         *     <li>When a message parameter is omitted: {@code error at position}</li>
         * </ul>
         *
         * @param error an error.
         * @param position a position in a script where a problem has occurred.
         * @param message an additional message.
         */
        ScriptException(String error, ScriptPosition position, @Nullable String message) {
            super(scriptErrorMessage(error, position, message));
        }

        /**
         * Similar to {@link ScriptException#ScriptException(String, ScriptPosition, String)} but allow to pass a cause.
         *
         * @param error an error.
         * @param position a position in a script where a problem has occurred.
         * @param message an additional message.
         * @param cause a cause of this exception.
         */
        ScriptException(String error, ScriptPosition position, @Nullable String message, @Nullable Throwable cause) {
            super(scriptErrorMessage(error, position, message), cause);
        }

        private static String scriptErrorMessage(String message, ScriptPosition position, @Nullable String details) {
            if (details == null || details.isEmpty()) {
                return String.format("%s at %s", message, position);
            } else {
                return String.format("%s at %s: %s", message, position, details);
            }
        }
    }

    @FunctionalInterface
    interface ParseCommand {

        Command parse(Script script, ScriptContext ctx, String[] tokens) throws IOException;
    }

    private abstract static class Command {
        final ScriptPosition posDesc;

        Command(ScriptPosition posDesc) {
            this.posDesc = posDesc;
        }

        abstract void execute(ScriptContext ctx);
    }

    private static final class OnlyIf extends Command {

        String[] condition;

        Command command;

        OnlyIf(Script script, ScriptContext ctx, String[] tokens) throws IOException {
            super(script.scriptPosition());

            var nextCommand = script.nextCommand();
            if (nextCommand == null) {
                throw script.reportInvalidCommand("Expected a next command", tokens);
            }

            this.command = nextCommand;
            this.condition = tokens;
        }

        @Override
        void execute(ScriptContext ctx) {
            if (condition[1].equals(ctx.engineName)) {
                command.execute(ctx);
            }
        }
    }

    private static final class SkipIf extends Command {

        String[] condition;

        Command command;

        SkipIf(Script script, ScriptContext ctx, String[] tokens) {
            super(script.scriptPosition());

            var nextCommand = script.nextCommand();
            if (nextCommand == null) {
                throw script.reportInvalidCommand("Expected a next command", tokens);
            }

            this.command = nextCommand;
            this.condition = tokens;
        }

        @Override
        void execute(ScriptContext ctx) {
            if (condition[1].equals(ctx.engineName)) {
                return;
            }
            command.execute(ctx);
        }
    }

    private static final class Loop extends Command {
        final List<Command> cmds = new ArrayList<>();

        int begin;

        int end;

        String var;

        Loop(Script script, ScriptContext ctx, String[] cmdTokens) throws IOException {
            super(script.scriptPosition());

            try {
                var = cmdTokens[1];
                begin = Integer.parseInt(cmdTokens[2]);
                end = Integer.parseInt(cmdTokens[3]);
            } catch (Exception e) {
                throw script.reportInvalidCommand("Unexpected loop syntax", cmdTokens, e);
            }

            while (script.ready()) {
                Command cmd = script.nextCommand();
                if (cmd instanceof EndLoop) {
                    break;
                }

                cmds.add(cmd);
            }
        }

        @Override
        void execute(ScriptContext ctx) {
            for (int i = begin; i < end; ++i) {
                ctx.loopVars.put(var, i);

                for (Command cmd : cmds) {
                    cmd.execute(ctx);
                }
            }
        }
    }

    private static final class EndLoop extends Command {

        EndLoop(Script script, ScriptContext ctx, String[] tokens) {
            super(script.scriptPosition());
            if (tokens.length > 1) {
                throw new IllegalArgumentException("EndLoop accepts no arguments: " + Arrays.toString(tokens));
            }
        }

        @Override
        void execute(ScriptContext ctx) {
            // No-op.
        }
    }

    private static final class Statement extends Command {
        List<String> queries;

        ExpectedStatementStatus expected;

        Statement(Script script, ScriptContext ctx, String[] cmd) throws IOException {
            super(script.scriptPosition());

            String expectedStatus = cmd[1];
            switch (expectedStatus) {
                case "ok":
                    expected = ExpectedStatementStatus.ok();
                    break;
                case "error":
                    expected = ExpectedStatementStatus.error();
                    break;
                default:
                    if (expectedStatus.startsWith("error:")) {
                        String[] errorMessage = Arrays.copyOfRange(cmd, 2, cmd.length);
                        expected = ExpectedStatementStatus.error(String.join(" ", errorMessage).trim());
                    } else {
                        throw script.reportInvalidCommand("Statement argument should be 'ok' or 'error'", cmd);
                    }
            }

            queries = new ArrayList<>();

            while (script.ready()) {
                String s = script.nextLine();

                if (Strings.isNullOrEmpty(s)) {
                    break;
                }

                queries.add(s);
            }
        }

        @Override
        void execute(ScriptContext ctx) {
            for (String qry : queries) {
                String[] toks = qry.split("\\s+");

                if ("PRAGMA".equals(toks[0])) {
                    String[] pragmaParams = toks[1].split("=");

                    if ("null".equals(pragmaParams[0])) {
                        ctx.nullLbl = pragmaParams[1];
                    } else {
                        ctx.log.info("Ignore: " + Arrays.toString(pragmaParams));
                    }

                    continue;
                }

                if (expected.successful) {
                    try {
                        sql(ctx, qry);
                    } catch (Throwable e) {
                        Assertions.fail("Not expected result at: " +  posDesc + ". Statement: " + qry, e);
                    }
                } else {
                    Throwable err = Assertions.assertThrows(Throwable.class, () -> sql(ctx, qry),
                            "Not expected result at: " + posDesc + ". Statement: " + qry + ". Error: " + expected.errorMessage);

                    assertThat("Not expected result at: " + posDesc + ". Statement: " + qry
                            + ". Expected: " + expected.errorMessage, err.getMessage(), expected.errorMessage);
                }
            }
        }

        @Override
        public String toString() {
            return "Statement ["
                    + "queries=" + queries
                    + ", expected=" + expected
                    + ']';
        }
    }

    private static final class Query extends Command {

        /** Comparator for "valuesort" sort mode. */
        private static final Comparator<Object> ITM_COMPARATOR = (r1, r2) -> {
            String s1 = String.valueOf(r1);
            String s2 = String.valueOf(r2);

            return s1.compareTo(s2);
        };

        final List<ColumnType> resTypes = new ArrayList<>();

        final StringBuilder sql = new StringBuilder();

        List<List<String>> expectedRes;

        String expectedHash;

        int expectedRows;

        /** Sorting algo. */
        SortType sortType = SortType.NOSORT;

        /** Equality label. */
        String eqLabel;

        Query(Script script, ScriptContext ctx, String[] cmd) throws IOException {
            super(script.scriptPosition());

            String resTypesChars = cmd[1];

            if (cmd.length > 2) {
                sortType = SortType.valueOf(cmd[2].toUpperCase());
            }

            if (cmd.length > 3) {
                eqLabel = cmd[3].toLowerCase();
            }

            for (int i = 0; i < resTypesChars.length(); i++) {
                switch (resTypesChars.charAt(i)) {
                    case 'I':
                        resTypes.add(ColumnType.I);

                        break;

                    case 'R':
                        resTypes.add(ColumnType.R);

                        break;

                    case 'T':
                        resTypes.add(ColumnType.T);

                        break;

                    default:
                        throw script.reportInvalidCommand("Unknown type character '" + resTypesChars.charAt(i) + "'", cmd);
                }
            }

            if (CollectionUtils.nullOrEmpty(resTypes)) {
                throw script.reportInvalidCommand("Missing type string", cmd);
            }

            // Read SQL query
            while (script.ready()) {
                String line = script.nextLine();

                // Check for typos. Otherwise we are going to get very confusing errors down the road.
                if (line.startsWith("--")) {
                    if (line.equals("----")) {
                        break;
                    } else {
                        throw script.reportError("Invalid query terminator sequence", line);
                    }
                }

                if (sql.length() > 0) {
                    sql.append("\n");
                }

                sql.append(line);
            }

            // Read expected results
            String s = script.nextLineWithoutTrim();
            Matcher m = HASHING_PTRN.matcher(s);

            if (m.matches()) {
                // Expected results are hashing
                expectedRows = Integer.parseInt(m.group(1));
                expectedHash = m.group(2);
            } else {
                // Read expected results tuples.
                expectedRes = new ArrayList<>();

                boolean singleValOnLine = false;

                List<String> row = new ArrayList<>();

                while (!Strings.isNullOrEmpty(s)) {
                    String[] vals = s.split("\\t");

                    if (!singleValOnLine && vals.length == 1 && vals.length != resTypes.size()) {
                        singleValOnLine = true;
                    }

                    if (vals.length != resTypes.size() && !singleValOnLine) {
                        var message = " [row=\"" + s + "\", types=" + resTypes + ']';
                        throw script.reportError("Invalid columns count at the result", message);
                    }

                    try {
                        if (singleValOnLine) {
                            row.add(ctx.nullLbl.equals(vals[0]) ? null : vals[0]);

                            if (row.size() == resTypes.size()) {
                                expectedRes.add(row);

                                row = new ArrayList<>();
                            }
                        } else {
                            for (String val : vals) {
                                row.add(ctx.nullLbl.equals(val) ? null : val);
                            }

                            expectedRes.add(row);
                            row = new ArrayList<>();
                        }
                    } catch (Exception e) {
                        var message = "[row=\"" + s + "\", types=" + resTypes + ']';
                        throw script.reportError("Cannot parse expected results", message, e);
                    }

                    s = script.nextLineWithoutTrim();
                }
            }
        }

        @Override
        void execute(ScriptContext ctx) {
            List<List<?>> res = sql(ctx, sql.toString());

            checkResult(ctx, res);
        }

        void checkResult(ScriptContext ctx, List<List<?>> res) {
            if (sortType == SortType.ROWSORT) {
                res.sort(ctx.rowComparator);

                if (expectedRes != null) {
                    expectedRes.sort(ctx.rowComparator);
                }
            } else if (sortType == SortType.VALUESORT) {
                List<Object> flattenRes = new ArrayList<>();

                res.forEach(flattenRes::addAll);

                flattenRes.sort(ITM_COMPARATOR);

                List<List<?>> resSizeAware = new ArrayList<>();
                int rowLen = resTypes.size();
                List<Object> rowRes = new ArrayList<>(rowLen);

                for (Object item : flattenRes) {
                    rowRes.add(item);

                    if (--rowLen == 0) {
                        resSizeAware.add(rowRes);
                        rowRes = new ArrayList<>(rowLen);
                        rowLen = resTypes.size();
                    }
                }

                res = resSizeAware;
            }

            if (expectedHash != null) {
                checkResultsHashed(ctx, res);
            } else {
                checkResultTuples(ctx, res);
            }
        }

        private void checkResultTuples(ScriptContext ctx, List<List<?>> res) {
            if (expectedRes.size() != res.size()) {
                throw new AssertionError("Invalid results rows count at: " + posDesc
                        + ". [expectedRows=" + expectedRes.size() + ", actualRows=" + res.size()
                        + ", expected=" + expectedRes + ", actual=" + res + ']');
            }

            for (int i = 0; i < expectedRes.size(); ++i) {
                List<String> expectedRow = expectedRes.get(i);
                List<?> row = res.get(i);

                if (row.size() != expectedRow.size()) {
                    throw new AssertionError("Invalid columns count at: " + posDesc
                            + ". [expected=" + expectedRes + ", actual=" + res + ']');
                }

                for (int j = 0; j < expectedRow.size(); ++j) {
                    checkEquals(ctx,
                            "Not expected result at: " + posDesc
                                    + ". [row=" + i + ", col=" + j
                                    + ", expected=" + expectedRow.get(j) + ", actual=" + SqlScriptRunner.toString(ctx, row.get(j)) + ']',
                            expectedRow.get(j),
                            row.get(j)
                    );
                }
            }
        }

        private void checkEquals(ScriptContext ctx, String msg, String expectedStr, Object actual) {
            if (actual == null && (expectedStr == null || ctx.nullLbl.equalsIgnoreCase(expectedStr))) {
                return;
            }

            if (actual != null ^ expectedStr != null) {
                throw new AssertionError(msg);
            }

            // Alternative values for boolean "0" and "1".
            if (actual instanceof Boolean && expectedStr.equals((Boolean) actual ? "1" : "0")) {
                return;
            }

            if (actual instanceof Number) {
                if ("NaN".equals(expectedStr) || expectedStr.endsWith("Infinity")) {
                    if (!expectedStr.equals(String.valueOf(actual))) {
                        throw new AssertionError(msg);
                    }
                } else {
                    BigDecimal actDec = new BigDecimal(String.valueOf(actual));
                    BigDecimal expDec = new BigDecimal(expectedStr);

                    if (actDec.compareTo(expDec) != 0) {
                        throw new AssertionError(msg);
                    }
                }
            } else if (actual instanceof Map) {
                String actualStr = mapToString(ctx, (Map<? extends Comparable, ?>) actual);

                if (!expectedStr.equals(actualStr)) {
                    throw new AssertionError(msg);
                }
            } else {
                if (!String.valueOf(expectedStr).equals(SqlScriptRunner.toString(ctx, actual))
                        && !("(empty)".equals(expectedStr) && SqlScriptRunner.toString(ctx, actual).isEmpty())) {
                    throw new AssertionError(msg);
                }
            }
        }

        private void checkResultsHashed(ScriptContext ctx, List<List<?>> res) {
            Objects.requireNonNull(res, "empty result set");

            ctx.messageDigest.reset();

            for (List<?> row : res) {
                for (Object col : row) {
                    ctx.messageDigest.update(SqlScriptRunner.toString(ctx, col).getBytes(Charset.forName(UTF_8.name())));
                    ctx.messageDigest.update(LINE_SEPARATOR_BYTES);
                }
            }

            String res0 = IgniteUtils.toHexString(ctx.messageDigest.digest());

            if (eqLabel != null) {
                if (res0.equals(expectedHash)) {
                    ctx.eqResStorage.computeIfAbsent(eqLabel, k -> new ArrayList<>()).add(sql.toString());
                } else {
                    Collection<String> eq = ctx.eqResStorage.get(eqLabel);

                    if (eq != null) {
                        throw new AssertionError("Results of queries need to be equal: " + eq
                                + "\n and \n" + sql);
                    }
                }
            }

            if (!res0.equalsIgnoreCase(expectedHash)) {
                throw new AssertionError("Unexpected hash result, error at: " + posDesc
                        + ", expected=" + expectedHash + ", calculated=" + res0
                        + ", expectedRows=" + expectedRows + ", returnedRows=" + res.size() * res.get(0).size());
            }
        }

        @Override
        public String toString() {
            return "Query ["
                    + "sql=" + sql
                    + ", resTypes=" + resTypes
                    + ", expectedRes=" + expectedRes
                    + ", expectedHash='" + expectedHash + '\''
                    + ", expectedRows=" + expectedRows
                    + ", sortType=" + sortType
                    + ", eqLabel='" + eqLabel + '\''
                    + ']';
        }
    }

    private static class ExpectedStatementStatus {

        private final boolean successful;

        private final org.hamcrest.Matcher<String> errorMessage;

        ExpectedStatementStatus(boolean successful, @Nullable org.hamcrest.Matcher<String> errorMessage) {
            if (successful && errorMessage != null) {
                throw new IllegalArgumentException("Successful status with error message: " + errorMessage);
            }
            this.successful = successful;
            this.errorMessage = errorMessage;
        }

        static ExpectedStatementStatus ok() {
            return new ExpectedStatementStatus(true, null);
        }

        static ExpectedStatementStatus error() {
            return new ExpectedStatementStatus(false, CoreMatchers.any(String.class));
        }

        static ExpectedStatementStatus error(String errorMessage) {
            return new ExpectedStatementStatus(false, CoreMatchers.containsString(errorMessage));
        }

        @Override
        public String toString() {
            if (successful) {
                return "ok";
            } else {
                return "error:" + errorMessage;
            }
        }
    }

    private enum ColumnType {
        I,
        T,
        R
    }

    private enum SortType {
        ROWSORT,
        VALUESORT,
        NOSORT
    }
}
