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

package org.apache.ignite.internal.sql.sqllogic;

import static org.apache.ignite.internal.util.StringUtils.nullOrEmpty;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.sql.sqllogic.SqlScriptRunner.RunnerRuntime;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * ScriptContext maintains the state of a script execution and provides access
 * to the query executor and other dependencies to script commands.
 */
final class ScriptContext {

    /** NULL label. */
    private static final String NULL = "NULL";

    /** Query engine. */
    private final IgniteSql ignSql;

    /** Logger. */
    final IgniteLogger log;

    /** Database engine name. **/
    final String engineName;

    /** Loop variables. */
    final Map<String, String> loopVars = new HashMap<>();

    /** String presentation of null's. */
    String nullLbl = NULL;

    /** Time zone to use. */
    @Nullable ZoneId timeZone;

    /** Equivalent results store. */
    final Map<String, Collection<String>> eqResStorage = new HashMap<>();

    /** Hash algo. */
    final MessageDigest messageDigest;

    ScriptContext(RunnerRuntime runnerRuntime) {
        this.log = runnerRuntime.log();
        this.engineName = runnerRuntime.engineName();
        this.ignSql = runnerRuntime.sql();

        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    List<List<?>> executeQuery(String sql) {
        sql = replaceVars(sql);

        log.info("Execute: {}", sql);

        long startNanos = System.nanoTime();

        Statement.StatementBuilder statement = ignSql.statementBuilder().query(sql);
        if (timeZone != null) {
            statement = statement.timeZoneId(timeZone);
        }

        try (ResultSet<SqlRow> rs = ignSql.execute((Transaction) null, statement.build())) {
            if (rs.hasRowSet()) {
                List<List<?>> out = new ArrayList<>();

                rs.forEachRemaining(r -> {
                    List<?> row = new ArrayList<>();

                    for (int i = 0; i < rs.metadata().columns().size(); ++i) {
                        row.add(r.value(i));
                    }

                    out.add(row);
                });

                return out;
            } else if (rs.affectedRows() != -1) {
                return Collections.singletonList(Collections.singletonList(rs.affectedRows()));
            } else {
                return Collections.singletonList(Collections.singletonList(rs.wasApplied()));
            }
        } finally {
            long tookNanos = System.nanoTime() - startNanos;
            log.info("Execution took {} ms", TimeUnit.NANOSECONDS.toMillis(tookNanos));
        }
    }

    String replaceVars(String str) {
        if (nullOrEmpty(str)) {
            return str;
        }
        if (!loopVars.isEmpty()) {
            for (Map.Entry<String, String> loopVar : loopVars.entrySet()) {
                str = str.replaceAll("\\$\\{" + loopVar.getKey() + "\\}", loopVar.getValue());
            }
        }
        return str;
    }
}
