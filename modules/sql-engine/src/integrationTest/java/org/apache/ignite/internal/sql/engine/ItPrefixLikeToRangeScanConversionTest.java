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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsSubPlan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsTableScan;

import com.google.common.collect.Streams;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Set of tests to validation conversion of prefix LIKE predicate to range scan.
 */
public class ItPrefixLikeToRangeScanConversionTest extends BaseSqlIntegrationTest {
    private static final String QUERY_BASE = "SELECT val FROM t WHERE int_val = 1 AND val LIKE ?";
    private static String[] indexNames;

    @BeforeAll
    static void setupSchema() {
        sqlScript("CREATE TABLE t(id INT PRIMARY KEY, int_val INT, val VARCHAR);"
                + "CREATE INDEX t_val_asc_nulls_first_idx ON t (val ASC NULLS FIRST);"
                + "CREATE INDEX t_val_asc_nulls_last_idx ON t (val ASC NULLS LAST);"
                + "CREATE INDEX t_val_desc_nulls_first_idx ON t (val DESC NULLS FIRST);"
                + "CREATE INDEX t_val_desc_nulls_last_idx ON t (val DESC NULLS LAST);"
                + "CREATE INDEX t_int_val_asc_val_asc_nulls_first_idx ON t (int_val ASC, val ASC NULLS FIRST);"
                + "CREATE INDEX t_int_val_asc_val_asc_nulls_last_idx ON t (int_val ASC, val ASC NULLS LAST);"
                + "CREATE INDEX t_int_val_asc_val_desc_nulls_first_idx ON t (int_val ASC, val DESC NULLS FIRST);"
                + "CREATE INDEX t_int_val_asc_val_desc_nulls_last_idx ON t (int_val ASC, val DESC NULLS LAST);"
        );

        indexNames = new String[] {
                "t_val_asc_nulls_first_idx",
                "t_val_asc_nulls_last_idx",
                "t_val_desc_nulls_first_idx",
                "t_val_desc_nulls_last_idx",
                "t_int_val_asc_val_asc_nulls_first_idx",
                "t_int_val_asc_val_asc_nulls_last_idx",
                "t_int_val_asc_val_desc_nulls_first_idx",
                "t_int_val_asc_val_desc_nulls_last_idx"
        };

        String[] values = {
                "foo", "fooa", "food", "fooz", "fooaa", "fooda", "fooza", "foo_b", "foo_bar", "foo%b", "foo%bar",
                "fop", "fopa",
                null, null, null,
                threeMaxCharString(), threeMaxCharString() + "a", threeMaxCharString() + "b", threeMaxCharString() + "aa"
        };

        for (int i = 0; i < values.length; i++) {
            sql("INSERT INTO t VALUES (?, 1, ?)", i, values[i]);
        }
    }

    private static Stream<Arguments> indexNames() {
        return Streams.concat(
                Stream.of((String) null),
                Stream.of(indexNames)
        ).map(Arguments::of);
    } 

    @ParameterizedTest
    @MethodSource("indexNames")
    void simplePrefixMatchesAll(@Nullable String indexName) {
        Matcher<String> planMather = indexOrTableScanMather(indexName);

        assertQuery(appendForceIndexHint(QUERY_BASE, indexName))
                .withParam("foo%")
                .matches(planMather)
                .returns("foo")
                .returns("fooa")
                .returns("food")
                .returns("fooz")
                .returns("fooaa")
                .returns("fooda")
                .returns("fooza")
                .returns("foo_b")
                .returns("foo_bar")
                .returns("foo%b")
                .returns("foo%bar")
                .check();
    }

    @ParameterizedTest
    @MethodSource("indexNames")
    void simplePrefixMatchesOne(@Nullable String indexName) {
        Matcher<String> planMather = indexOrTableScanMather(indexName);

        assertQuery(appendForceIndexHint(QUERY_BASE, indexName))
                .withParam("foo_")
                .matches(planMather)
                .returns("fooa")
                .returns("food")
                .returns("fooz")
                .check();
    }

    @ParameterizedTest
    @MethodSource("indexNames")
    void simplePrefixMatchesAllEscaped(@Nullable String indexName) {
        Matcher<String> planMather = indexOrTableScanMather(indexName);

        assertQuery(appendForceIndexHint(QUERY_BASE + " ESCAPE '^'", indexName))
                .withParam("foo^%%")
                .matches(planMather)
                .returns("foo%b")
                .returns("foo%bar")
                .check();
    }

    @ParameterizedTest
    @MethodSource("indexNames")
    void simplePrefixMatchesOneEscaped(@Nullable String indexName) {
        Matcher<String> planMather = indexOrTableScanMather(indexName);

        assertQuery(appendForceIndexHint(QUERY_BASE + " ESCAPE '^'", indexName))
                .withParam("foo^__")
                .matches(planMather)
                .returns("foo_b")
                .check();
    }

    @ParameterizedTest
    @MethodSource("indexNames")
    void nullPattern(@Nullable String indexName) {
        Matcher<String> planMather = indexOrTableScanMather(indexName);

        assertQuery(appendForceIndexHint(QUERY_BASE, indexName))
                .withParam(null)
                .matches(planMather)
                .returnNothing()
                .check();
    }

    @ParameterizedTest
    @MethodSource("indexNames")
    void maxCharPrefixMatchesAll(@Nullable String indexName) {
        Matcher<String> planMather = indexOrTableScanMather(indexName);

        assertQuery(appendForceIndexHint(QUERY_BASE, indexName))
                .withParam(threeMaxCharString() + "%")
                .matches(planMather)
                .returns(threeMaxCharString())
                .returns(threeMaxCharString() + "a")
                .returns(threeMaxCharString() + "b")
                .returns(threeMaxCharString() + "aa")
                .check();
    }

    @ParameterizedTest
    @MethodSource("indexNames")
    void maxCharPrefixMatchesOne(@Nullable String indexName) {
        Matcher<String> planMather = indexOrTableScanMather(indexName);

        assertQuery(appendForceIndexHint(QUERY_BASE, indexName))
                .withParam(threeMaxCharString() + "_")
                .matches(planMather)
                .returns(threeMaxCharString() + "a")
                .returns(threeMaxCharString() + "b")
                .check();
    }

    private static @NotNull Matcher<String> indexOrTableScanMather(@Nullable String indexName) {
        return indexName == null
                ? containsTableScan("PUBLIC", "T")
                : Matchers.allOf(
                        containsIndexScan("PUBLIC", "T", indexName.toUpperCase()),
                        containsSubPlan("searchBounds")
                );
    }

    private static String threeMaxCharString() {
        return ("" + Character.MAX_VALUE).repeat(3);
    }

    private static String appendForceIndexHint(String query, @Nullable String indexName) {
        if (indexName == null) {
            query = query.replace("SELECT", "SELECT /*+ no_index */");
        } else {
            query = query.replace("SELECT", "SELECT /*+ force_index(" + indexName + ") */");
        }

        return query;
    }
}
