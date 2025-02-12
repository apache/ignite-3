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

package org.apache.ignite.internal.sql.engine.planner;

import static org.apache.ignite.internal.sql.engine.planner.AbstractTpcQueryPlannerTest.TpcSuiteInfo;

import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsHelper;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsTables;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests ensures a planner generates optimal plan for TPC-DS queries.
 */
// TODO https://issues.apache.org/jira/browse/IGNITE-21986 validate other query plans and make test parameterized.
@TpcSuiteInfo(
        tables = TpcdsTables.class,
        queryLoader = "getQueryString",
        planLoader = "getQueryPlan"
)
public class TpcdsQueryPlannerTest extends AbstractTpcQueryPlannerTest {
    @ParameterizedTest
    @ValueSource(strings = "48")
    public void test(String queryId) {
        validateQueryPlan(queryId);
    }

    @SuppressWarnings("unused") // used reflectively by AbstractTpcQueryPlannerTest
    static String getQueryString(String queryId) {
        return TpcdsHelper.getQuery(queryId);
    }

    @SuppressWarnings("unused") // used reflectively by AbstractTpcQueryPlannerTest
    static String getQueryPlan(String queryId) {
        // variant query ends with "v"
        boolean variant = queryId.endsWith("v");
        int numericId;

        if (variant) {
            String idString = queryId.substring(0, queryId.length() - 1);
            numericId = Integer.parseInt(idString);
        } else {
            numericId = Integer.parseInt(queryId);
        }

        if (variant) {
            var variantQueryFile = String.format("tpcds/plan/variant_q%d.plan", numericId);
            return loadFromResource(variantQueryFile);
        } else {
            var queryFile = String.format("tpcds/plan/q%s.plan", numericId);
            return loadFromResource(queryFile);
        }
    }
}
