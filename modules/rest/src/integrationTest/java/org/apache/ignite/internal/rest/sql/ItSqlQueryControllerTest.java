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

package org.apache.ignite.internal.rest.sql;

import static io.micronaut.http.HttpRequest.DELETE;
import static io.micronaut.http.HttpStatus.NOT_FOUND;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.rest.api.sql.SqlQueryInfo;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link SqlQueryController}.
 */
@MicronautTest
public class ItSqlQueryControllerTest extends ClusterPerClassIntegrationTest {
    private static final String SQL_QUERY_URL = "/management/v1/sql/";

    @Inject
    @Client("http://localhost:10300" + SQL_QUERY_URL)
    HttpClient client;

    @Test
    void shouldReturnAllSqlQueries() {
        String sql = "SELECT x FROM TABLE(SYSTEM_RANGE(1, 100));";

        // run query with results pageSize=1
        try (ResultSet<SqlRow> ignored = runQuery(sql)) {

            // the query must be active until cursor is closed
            Map<UUID, SqlQueryInfo> queries = getSqlQueries(client);

            assertThat(queries, aMapWithSize(2));
            SqlQueryInfo queryInfo = queries.entrySet().stream()
                    .filter(e -> e.getValue().sql().equals(sql))
                    .findFirst().map(Entry::getValue).orElse(null);

            assertThat(queryInfo, notNullValue());
            assertThat(queryInfo.schema(), is("PUBLIC"));
            assertThat(queryInfo.type(), is("QUERY"));
        }
    }

    @Test
    void shouldReturnSingleQuery() {
        String sql = "SELECT x FROM TABLE(SYSTEM_RANGE(1, 100));";

        try (ResultSet<SqlRow> ignored = runQuery(sql)) {
            // the query must be active until cursor is closed
            Map<UUID, SqlQueryInfo> queries = getSqlQueries(client);

            assertThat(queries, aMapWithSize(2));
            SqlQueryInfo queryInfo = queries.entrySet().stream()
                    .filter(e -> e.getValue().sql().equals(sql))
                    .findFirst().map(Entry::getValue).orElse(null);

            assertThat(queryInfo, notNullValue());

            SqlQueryInfo query = getSqlQuery(client, queryInfo.id());
            assertThat(query.id(), is(queryInfo.id()));
            assertThat(query.sql(), is(queryInfo.sql()));
            assertThat(query.type(), is(queryInfo.type()));
            assertThat(query.startTime(), is(queryInfo.startTime()));
        }
    }

    @Test
    void shouldKillSqlQuery() {
        String sql = "SELECT x FROM TABLE(SYSTEM_RANGE(1, 100));";

        try (ResultSet<SqlRow> ignored = runQuery(sql)) {
            // the query must be active until cursor is closed
            Map<UUID, SqlQueryInfo> queries = getSqlQueries(client);

            assertThat(queries, aMapWithSize(2));
            SqlQueryInfo queryInfo = queries.entrySet().stream()
                    .filter(e -> e.getValue().sql().equals(sql))
                    .findFirst().map(Entry::getValue).orElse(null);

            assertThat(queryInfo, notNullValue());

            killSqlQuery(client, queryInfo.id());

            assertThrowsProblem(
                    () -> getSqlQuery(client, queryInfo.id()),
                    isProblem().withStatus(NOT_FOUND).withDetail("Sql query not found [queryId=" + queryInfo.id() + "]")
            );
        }
    }

    @Test
    void shouldReturnProblemIfRetrieveNonExistingSqlQuery() {
        UUID queryId = UUID.randomUUID();

        assertThrowsProblem(
                () -> getSqlQuery(client, queryId),
                isProblem().withStatus(NOT_FOUND).withDetail("Sql query not found [queryId=" + queryId + "]")
        );
    }

    @Test
    void shouldReturnProblemIfKillNonExistingSqlQuery() {
        UUID queryId = UUID.randomUUID();

        assertThrowsProblem(
                () -> killSqlQuery(client, queryId),
                isProblem().withStatus(NOT_FOUND).withDetail("Sql query not found [queryId=" + queryId + "]")
        );
    }

    private static ResultSet<SqlRow> runQuery(String sql) {
        IgniteSql igniteSql = CLUSTER.aliveNode().sql();
        Statement stmt = igniteSql.statementBuilder().query(sql).pageSize(1).build();

        return CLUSTER.aliveNode().sql().execute((Transaction) null, stmt);
    }

    private static Map<UUID, SqlQueryInfo> getSqlQueries(HttpClient client) {
        List<SqlQueryInfo> sqlQueries = client.toBlocking()
                .retrieve(HttpRequest.GET("/queries"), Argument.listOf(SqlQueryInfo.class));

        return sqlQueries.stream().collect(Collectors.toMap(SqlQueryInfo::id, s -> s));
    }

    private static SqlQueryInfo getSqlQuery(HttpClient client, UUID queryId) {
        return client.toBlocking().retrieve(HttpRequest.GET("/queries/" + queryId), SqlQueryInfo.class);
    }

    private static void killSqlQuery(HttpClient client, UUID queryId) {
        client.toBlocking().exchange(DELETE("/queries/" + queryId));
    }
}
